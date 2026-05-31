import { WatchPanel } from "./watch-panel";

export default async function WatchPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <WatchPanel id={id} />;
}

