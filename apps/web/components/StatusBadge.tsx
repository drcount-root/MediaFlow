import type { VideoStatus } from "@/lib/api";

export function StatusBadge({ status }: { status: VideoStatus }) {
  return <span className={`status ${status}`}>{status}</span>;
}

