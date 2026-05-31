"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { Play, RotateCw } from "lucide-react";
import { getVideo, type VideoItem } from "@/lib/api";
import { StatusBadge } from "@/components/StatusBadge";

export function VideoStatusPanel({ id }: { id: string }) {
  const [video, setVideo] = useState<VideoItem | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;

    async function load() {
      try {
        const nextVideo = await getVideo(id);
        if (!active) {
          return;
        }
        setVideo(nextVideo);
        setError(null);
      } catch (err) {
        if (active) {
          setError(err instanceof Error ? err.message : "Could not load video.");
        }
      }
    }

    load();
    const timer = setInterval(() => {
      if (video?.status === "ready" || video?.status === "failed") {
        return;
      }
      load();
    }, 2000);

    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [id, video?.status]);

  return (
    <section className="panel watch-layout">
      {error ? <div className="alert error">{error}</div> : null}
      {!video ? <div className="alert">Loading video status.</div> : null}
      {video ? (
        <>
          <div className="page-title">
            <div>
              <h1>{video.title}</h1>
              <p>{video.description || "No description"}</p>
            </div>
            <StatusBadge status={video.status} />
          </div>
          {video.status === "ready" ? (
            <Link href={`/watch/${video.id}`} className="primary-link">
              <Play size={16} />
              Watch
            </Link>
          ) : null}
          {video.status === "failed" ? <div className="alert error">{video.errorMessage || "Processing failed."}</div> : null}
          {video.status !== "ready" && video.status !== "failed" ? (
            <div className="alert">
              <RotateCw size={15} /> Processing is still running.
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  );
}

