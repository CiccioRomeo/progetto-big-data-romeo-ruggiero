import { TagsResponse } from "@/lib/types";
import WordCloud from "react-wordcloud";

interface WordCloudProps{
  tags: TagsResponse[];
}

export function WordCloudComponent({ tags }: WordCloudProps) {
  const data = tags.map(tag => ({
    text: tag.tagValue,
    value: tag.count,
  }));

  const options = {
    rotations: 2, 
    rotationAngles: [-90, 0] as [number, number], 
    scale: "sqrt" as const, 
    spiral: "archimedean" as const, 
    fontSizes: [10, 100] as [number, number],
  }

  return (
    <div style={{ width: "100%", height: "400px" }}>
      <WordCloud words={data} options={options} />
    </div>
  );
};
