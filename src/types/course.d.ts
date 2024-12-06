import { QuizProps } from "./quiz";

export type CourseProps = {
  _id: string;
  banner: {
    asset: {
      url: string;
    };
  };
  name: string;
  points: number;
  workload: number;
  description: string;
  premiumPoints: number;
};

export type FullCourseProps = CourseProps & {
  quiz: QuizProps;
};
