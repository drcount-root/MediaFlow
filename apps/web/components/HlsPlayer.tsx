"use client";

import Hls from "hls.js";
import { useEffect, useRef, useState } from "react";

export function HlsPlayer({ src }: { src: string }) {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    setError(null);

    if (video.canPlayType("application/vnd.apple.mpegurl")) {
      video.src = src;
      return;
    }

    if (!Hls.isSupported()) {
      setError("This browser cannot play HLS streams.");
      return;
    }

    const hls = new Hls();
    hls.loadSource(src);
    hls.attachMedia(video);
    hls.on(Hls.Events.ERROR, (_event, data) => {
      if (data.fatal) {
        setError("The stream could not be loaded.");
      }
    });

    return () => {
      hls.destroy();
    };
  }, [src]);

  return (
    <>
      <video ref={videoRef} className="player" controls playsInline />
      {error ? <div className="alert error">{error}</div> : null}
    </>
  );
}

