import { getCourseById } from "@/actions/courses/getId";
import { getUserDamage } from "@/actions/damage/getUserDamage";
import { getExamByLectureId } from "@/actions/exam/getExamByLectureId";
import { getLectureById } from "@/actions/lecture/getLectureById";
import { getQuizByLectureId } from "@/actions/quiz/getByLectureId";
import { getUserByClerk } from "@/actions/user/getUserByClerk";
import CompleteLectureButton from "@/components/CompleteLectureButton";
import NewExamButton from "@/components/NewExamButton";
import { defaultLifes } from "@/constants/exam";
import { LecturePageSerializer } from "@/serializers/course";
import { subHours } from "date-fns";
import { ChevronLeft } from "lucide-react";
import { PortableText } from "next-sanity";
import Link from "next/link";

type LecturePageProps = {
  params: {
    lectureId: string;
    courseId: string;
  };
};

const LecturePage = async ({ params }: LecturePageProps) => {
  const { lectureId, courseId } = params;
  const exam = await getExamByLectureId({ lectureId });

  const quiz = await getQuizByLectureId({ lectureId });

  const lecture = await getLectureById({ id: lectureId });

  const course = await getCourseById({ id: courseId });

  const user = await getUserByClerk();

  const userDamage = await getUserDamage({
    userId: user?.id!,
    from: subHours(new Date(), 12),
  });

  return (
    <div className="flex flex-col">
      <Link href={`/course/${courseId}`}>
        <div className="p-5 flex items-center border-b border-border text-primary justify-between">
          <ChevronLeft size={32} />

          <p className="text-xl text-primary">{course.name}</p>
        </div>
      </Link>
      <div className="h-[92svh] overflow-y-auto pb-20 max-w-[800px] mx-auto">
        <div className="p-5 space-y-2">
          <p className="text-2xl font-semibold text-primary">{course.name}</p>
          <PortableText
            value={lecture.content}
            components={LecturePageSerializer}
          />
        </div>

        {!quiz?._id ? (
          <CompleteLectureButton
            course={course}
            lectureId={lectureId}
            userId={user?.id!}
          />
        ) : (
          <NewExamButton
            courseId={courseId}
            lectureId={lectureId}
            userId={user?.id!}
            exam={exam}
            userLifes={defaultLifes - userDamage.length}
          />
        )}
      </div>
    </div>
  );
};

export default LecturePage;
