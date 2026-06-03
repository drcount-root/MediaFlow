"use client";

import Hls from "hls.js";
import { useEffect, useRef, useState } from "react";

type QualityLevel = {
  index: number;
  label: string;
  height: number;
};

export function HlsPlayer({ src }: { src: string }) {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const hlsRef = useRef<Hls | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [levels, setLevels] = useState<QualityLevel[]>([]);
  const [selectedLevel, setSelectedLevel] = useState(-1);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    setError(null);
    setNotice(null);
    setLevels([]);
    setSelectedLevel(-1);
    hlsRef.current?.destroy();
    hlsRef.current = null;

    if (!Hls.isSupported()) {
      if (video.canPlayType("application/vnd.apple.mpegurl")) {
        video.src = src;
        setNotice("This browser is using native HLS playback, so quality is managed automatically.");
        return;
      }
      setError("This browser cannot play HLS streams.");
      return;
    }

    const hls = new Hls();
    hlsRef.current = hls;
    hls.loadSource(src);
    hls.attachMedia(video);
    hls.on(Hls.Events.MANIFEST_PARSED, () => {
      const nextLevels = hls.levels
        .map((level, index) => ({
          index,
          height: level.height,
          label: level.height ? `${level.height}p` : `${Math.round(level.bitrate / 1000)} kbps`
        }))
        .sort((a, b) => b.height - a.height);

      setLevels(nextLevels);
    });
    hls.on(Hls.Events.ERROR, (_event, data) => {
      if (data.fatal) {
        setError("The stream could not be loaded.");
      }
    });

    return () => {
      hls.destroy();
      hlsRef.current = null;
    };
  }, [src]);

  function changeQuality(value: string) {
    const nextLevel = Number(value);
    setSelectedLevel(nextLevel);

    if (hlsRef.current) {
      hlsRef.current.currentLevel = nextLevel;
    }
  }

  return (
    <>
      <video ref={videoRef} className="player" controls playsInline />
      {levels.length > 0 ? (
        <div className="quality-bar">
          <label htmlFor="quality">Quality</label>
          <select id="quality" value={selectedLevel} onChange={(event) => changeQuality(event.target.value)}>
            <option value={-1}>Auto</option>
            {levels.map((level) => (
              <option key={level.index} value={level.index}>
                {level.label}
              </option>
            ))}
          </select>
        </div>
      ) : null}
      {notice ? <div className="alert">{notice}</div> : null}
      {error ? <div className="alert error">{error}</div> : null}
    </>
  );
}
