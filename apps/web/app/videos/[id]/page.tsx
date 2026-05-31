import { VideoStatusPanel } from "./status-panel";

export default async function VideoStatusPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <VideoStatusPanel id={id} />;
}

