"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { getPlayback, getVideo, type PlaybackResponse, type VideoItem } from "@/lib/api";
import { HlsPlayer } from "@/components/HlsPlayer";
import { StatusBadge } from "@/components/StatusBadge";

export function WatchPanel({ id }: { id: string }) {
  const [video, setVideo] = useState<VideoItem | null>(null);
  const [playback, setPlayback] = useState<PlaybackResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;

    async function load() {
      try {
        const [nextVideo, nextPlayback] = await Promise.all([getVideo(id), getPlayback(id)]);
        if (!active) {
          return;
        }
        setVideo(nextVideo);
        setPlayback(nextPlayback);
        setError(null);
      } catch (err) {
        if (active) {
          setError(err instanceof Error ? err.message : "Could not load playback.");
        }
      }
    }

    load();
    return () => {
      active = false;
    };
  }, [id]);

  return (
    <section className="watch-layout">
      {error ? (
        <div className="panel">
          <div className="alert error">{error}</div>
          <Link href={`/videos/${id}`} className="primary-link">
            View status
          </Link>
        </div>
      ) : null}
      {video && playback ? (
        <>
          <div className="page-title">
            <div>
              <h1>{video.title}</h1>
              <p>{video.description || "No description"}</p>
            </div>
            <StatusBadge status={video.status} />
          </div>
          <HlsPlayer src={playback.hlsUrl} />
          {video.variants?.length ? (
            <div className="panel">
              <div className="variants">
                {video.variants.map((variant) => (
                  <span className="variant-chip" key={variant.quality}>
                    {variant.quality} · {variant.width}x{variant.height}
                  </span>
                ))}
              </div>
            </div>
          ) : null}
        </>
      ) : null}
      {!error && !video ? <div className="alert">Loading playback.</div> : null}
    </section>
  );
}

