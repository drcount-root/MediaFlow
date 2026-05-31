import Link from "next/link";
import { Play, UploadCloud, Video } from "lucide-react";
import { StatusBadge } from "@/components/StatusBadge";
import { listVideos, type VideoItem } from "@/lib/api";

export default async function HomePage() {
  let videos: VideoItem[] = [];
  let error: string | null = null;

  try {
    videos = await listVideos();
  } catch (err) {
    error = err instanceof Error ? err.message : "Could not load videos.";
  }

  return (
    <>
      <div className="page-title">
        <div>
          <h1>Videos</h1>
          <p>Uploaded videos move from queued to ready after worker processing.</p>
        </div>
        <Link href="/upload" className="primary-link">
          <UploadCloud size={16} />
          Upload
        </Link>
      </div>

      {error ? <div className="alert error">{error}</div> : null}

      {!error && videos.length === 0 ? (
        <div className="panel">
          <div className="thumb">
            <Video size={34} />
          </div>
          <p className="meta">No videos yet.</p>
        </div>
      ) : null}

      <section className="grid">
        {videos.map((video) => (
          <Link key={video.id} href={video.status === "ready" ? `/watch/${video.id}` : `/videos/${video.id}`} className="video-card">
            <div className="thumb">
              {video.thumbnailUrl ? <img src={video.thumbnailUrl} alt="" /> : <Video size={32} />}
            </div>
            <StatusBadge status={video.status} />
            <h2 className="card-title">{video.title}</h2>
            <p className="meta">{video.durationSeconds ? `${Math.round(video.durationSeconds)}s` : "Processing metadata pending"}</p>
            {video.status === "ready" ? (
              <span className="primary-link">
                <Play size={15} />
                Watch
              </span>
            ) : null}
          </Link>
        ))}
      </section>
    </>
  );
}
