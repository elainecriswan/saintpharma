import { getUrlParam } from "@/tools/getUrlParm";
import { PortableTextComponents } from "next-sanity";
import Image from "next/image";

export const coursePageSerializer: PortableTextComponents = {
  types: {
    youtubeUrl: ({ value }) => {
      const videoId = getUrlParam(value.url, "v");

      return (
        <iframe
          className="rounded w-full h-[400px] tablet:h-[500px]"
          src={`https://www.youtube.com/embed/${videoId}`}
          allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
          allowFullScreen
        ></iframe>
      );
    },
    image: ({ value }) => {
      if (!value?.imageUrl) return null;
      return (
        <figure>
          <Image
            height={1000}
            width={1000}
            className="w-full h-auto max-w-[500px] mx-auto"
            src={value.imageUrl}
            alt={value.caption || "Imagem"}
          />
          {value.caption && <figcaption>{value.caption}</figcaption>}
        </figure>
      );
    },
  },
  block: {
    normal: ({ children }) => (
      <p className="my-4 [&>a]:text-primary">{children}</p>
    ),
    h1: ({ children }) => (
      <h1 className="text-4xl font-semibold text-primary">{children}</h1>
    ),
    h2: ({ children }) => (
      <h2 className="text-3xl font-semibold text-primary">{children}</h2>
    ),
    h3: ({ children }) => (
      <h3 className="text-2xl font-semibold text-primary">{children}</h3>
    ),
    h4: ({ children }) => (
      <h4 className="text-xl font-semibold text-primary">{children}</h4>
    ),
    h5: ({ children }) => (
      <h5 className="text-lg font-semibold text-primary">{children}</h5>
    ),
    h6: ({ children }) => (
      <h6 className="text-md font-semibold text-primary">{children}</h6>
    ),
  },

  marks: {
    strong: ({ children }) => <strong className="font-bold">{children}</strong>,
    em: ({ children }) => <em className="italic">{children}</em>,
  },
};
