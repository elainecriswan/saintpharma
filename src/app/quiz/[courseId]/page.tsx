import { getQuizByCourseId } from "@/actions/quiz/getByCourseId";
import Exam from "@/components/exam";
import Header from "@/components/Header";
import { useClerkUser } from "@/hooks/clerkUser";
import { redirect } from "next/navigation";

type QuizPageProps = {
  params: Promise<{
    courseId: string;
  }>;
};

const QuizPage = async ({ params }: QuizPageProps) => {
  const { courseId } = await params;

  if (!courseId) {
    redirect("/");
  }

  const quiz = await getQuizByCourseId({ id: courseId });
  const user = await useClerkUser();

  return (
    <div>
      <Header user={user} backIcon />
      <Exam quiz={quiz} />
    </div>
  );
};

export default QuizPage;
