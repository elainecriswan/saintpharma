import prisma from "@/lib/prisma";
import { NextResponse } from "next/server";

const MAX_DATABASE_ATTEMPTS = 3;
const RETRYABLE_DATABASE_CODES = new Set([
  "P1001", // database server unreachable
  "P1002", // database server timeout
  "P1008", // operation timeout
  "P1017", // server closed the connection
  "P2024", // connection pool timeout
  "P2034", // transaction conflict
  "P2037", // too many database connections
]);

const delay = (milliseconds: number) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

function getErrorDetails(error: unknown) {
  const value = error as { name?: string; code?: string; message?: string };
  return {
    name: value?.name || "UnknownError",
    code: value?.code,
    message: value?.message || String(error),
  };
}

function isRetryableDatabaseError(error: unknown) {
  const { code, message } = getErrorDetails(error);
  return (
    (!!code && RETRYABLE_DATABASE_CODES.has(code)) ||
    /timed out|timeout|connection reset|connection refused|connection closed|server has closed|temporarily unavailable|network error/i.test(
      message
    )
  );
}

function isUniqueConstraintError(error: unknown) {
  return getErrorDetails(error).code === "P2002";
}

function webhookErrorResponse(
  status: number,
  code: string,
  message: string,
  deliveryId: string,
  headers?: HeadersInit
) {
  return NextResponse.json(
    {
      success: false,
      error: { code, message },
      deliveryId,
    },
    { status, headers }
  );
}

async function withDatabaseRetries<T>(
  operationName: string,
  operation: () => Promise<T>
): Promise<T> {
  for (let attempt = 1; attempt <= MAX_DATABASE_ATTEMPTS; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      const retryable = isRetryableDatabaseError(error);
      if (!retryable || attempt === MAX_DATABASE_ATTEMPTS) {
        throw error;
      }

      const waitMilliseconds = 250 * 2 ** (attempt - 1);
      console.warn("Falha transitória no banco; repetindo webhook", {
        operationName,
        attempt,
        nextAttempt: attempt + 1,
        waitMilliseconds,
        ...getErrorDetails(error),
      });
      await delay(waitMilliseconds);
    }
  }

  throw new Error(`Falha ao executar operação do banco: ${operationName}`);
}

async function upsertClerkUser({
  clerkId,
  email,
  firstName,
  lastName,
  profileImage,
}: {
  clerkId: string;
  email: string;
  firstName: string;
  lastName: string | null;
  profileImage: string;
}) {
  const update = { clerkId, email, firstName, lastName, profileImage };
  const userByClerkId = await prisma.user.findUnique({
    where: { clerkId },
    select: { id: true },
  });

  if (userByClerkId) {
    return prisma.user.update({ where: { clerkId }, data: update });
  }

  try {
    // Email is unique; upsert makes duplicate Clerk deliveries safe.
    return await prisma.user.upsert({
      where: { email },
      update,
      create: update,
    });
  } catch (error) {
    // A parallel delivery may have inserted the same Clerk ID under a new email.
    if (!isUniqueConstraintError(error)) {
      throw error;
    }

    const userCreatedByConcurrentDelivery = await prisma.user.findUnique({
      where: { clerkId },
      select: { id: true },
    });
    if (!userCreatedByConcurrentDelivery) {
      throw error;
    }

    return prisma.user.update({ where: { clerkId }, data: update });
  }
}

export async function POST(req: Request) {
  const deliveryId = req.headers.get("svix-id") || "unknown";

  let body: any;
  try {
    body = await req.json();
  } catch {
    return webhookErrorResponse(
      400,
      "INVALID_JSON",
      "O corpo da entrega não contém um JSON válido.",
      deliveryId
    );
  }

  try {
    const { type, data } = body || {};

    console.log("Clerk webhook received:", { type });

    if (!data || typeof data !== "object") {
      return webhookErrorResponse(
        400,
        "INVALID_EVENT_DATA",
        "Os dados do evento estão ausentes ou inválidos.",
        deliveryId
      );
    }

    // Verificar se é um evento de delete
    if (type === "user.deleted") {
      const { id: clerkId } = data;
      
      if (!clerkId) {
        console.error("ClerkId não encontrado no evento de delete");
        return webhookErrorResponse(
          400,
          "USER_ID_REQUIRED",
          "O evento de exclusão não contém o ID do usuário.",
          deliveryId
        );
      }

      // Buscar usuário no banco pelo clerkId
      const user = await withDatabaseRetries("lookup deleted user", () =>
        prisma.user.findUnique({ where: { clerkId }, select: { id: true } })
      );

      if (!user) {
        // Delete deliveries can be replayed after the original already succeeded.
        return new NextResponse("Usuário já removido", { status: 200 });
      }

      // Deletar dados relacionados e o usuário
      await withDatabaseRetries("delete user", () => prisma.$transaction(async (tx) => {
        // Deletar tentativas de exame do usuário (deve ser deletado antes dos exames)
        await tx.examAttempt.deleteMany({
          where: { userId: user.id }
        });

        // Deletar certificados
        await tx.certificate.deleteMany({
          where: { userId: user.id }
        });

        // Deletar aulas do usuário
        await tx.userLecture.deleteMany({
          where: { userId: user.id }
        });

        // Deletar exames (após deletar ExamAttempt)
        await tx.exam.deleteMany({
          where: { userId: user.id }
        });

        // Deletar danos/vidas
        await tx.damage.deleteMany({
          where: { userId: user.id }
        });

        // Deletar o usuário
        await tx.user.deleteMany({
          where: { id: user.id }
        });
      }));

      console.log("Usuário do Clerk removido do banco de dados");
      return new NextResponse("Usuário deletado com sucesso", { status: 200 });
    }

    // Eventos de criação/atualização (comportamento original)
    if (type === "user.created" || type === "user.updated") {
      const { id, email_addresses, first_name, last_name, image_url } = data;
      const email = Array.isArray(email_addresses)
        ? email_addresses[0]?.email_address
        : undefined;

      if (typeof id !== "string" || !id || !email) {
        console.error("Webhook de usuário sem ID ou email válido", { type });
        return webhookErrorResponse(
          400,
          "USER_ID_AND_EMAIL_REQUIRED",
          "O evento de usuário precisa conter ID e endereço de email.",
          deliveryId
        );
      }

      // Processar firstName e lastName
      const firstName = first_name?.trim() || 'Usuário';
      const lastName = last_name?.trim() || null;

      await withDatabaseRetries("upsert Clerk user", () =>
        upsertClerkUser({
          clerkId: id,
          email,
          firstName,
          lastName,
          profileImage: image_url || "",
        })
      );

      return new NextResponse("Usuário processado com sucesso", { status: 200 });
    }

    // Evento não reconhecido
    console.log(`Evento não processado: ${type}`);
    return new NextResponse("Evento não processado", { status: 200 });
    
  } catch (error) {
    const retryable = isRetryableDatabaseError(error);
    console.error("Erro no webhook do Clerk:", {
      deliveryId,
      ...getErrorDetails(error),
    });

    if (retryable) {
      return webhookErrorResponse(
        503,
        "DATABASE_TEMPORARILY_UNAVAILABLE",
        "O banco de dados não respondeu após as tentativas automáticas. A entrega pode ser repetida.",
        deliveryId,
        { "Retry-After": "5" }
      );
    }

    return webhookErrorResponse(
      500,
      "WEBHOOK_PROCESSING_FAILED",
      "Não foi possível processar a entrega. Consulte os logs da função pelo ID da entrega.",
      deliveryId
    );
  }
}
