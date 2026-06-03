"use client";

import Hls from "hls.js";
import { Settings } from "lucide-react";
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
  const [activeLevel, setActiveLevel] = useState<number | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    setError(null);
    setNotice(null);
    setLevels([]);
    setSelectedLevel(-1);
    setActiveLevel(null);
    setMenuOpen(false);
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
    hls.on(Hls.Events.LEVEL_SWITCHED, (_event, data) => {
      setActiveLevel(data.level);
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
    setMenuOpen(false);

    if (hlsRef.current) {
      hlsRef.current.currentLevel = nextLevel;
    }
  }

  const activeLabel = activeLevel === null ? "detecting" : (levels.find((level) => level.index === activeLevel)?.label ?? "unknown");
  const selectedLabel = selectedLevel === -1 ? "Auto" : (levels.find((level) => level.index === selectedLevel)?.label ?? "Manual");
  const qualityStatus = selectedLevel === -1 ? `Auto (${activeLabel})` : selectedLabel;

  return (
    <>
      <div className="player-frame">
        <video ref={videoRef} className="player" controls playsInline />
        {levels.length > 0 ? (
          <div className="quality-menu">
            <button className="quality-trigger" type="button" onClick={() => setMenuOpen((open) => !open)} aria-expanded={menuOpen}>
              <Settings size={16} />
              <span>{qualityStatus}</span>
            </button>
            {menuOpen ? (
              <div className="quality-popover" role="menu" aria-label="Video quality">
                <button className={selectedLevel === -1 ? "quality-option active" : "quality-option"} type="button" onClick={() => changeQuality("-1")}>
                  <span>Auto</span>
                  <small>{activeLevel === null ? "detecting" : `currently ${activeLabel}`}</small>
                </button>
                <div className="quality-divider" />
                {levels.map((level) => (
                  <button
                    className={selectedLevel === level.index ? "quality-option active" : "quality-option"}
                    key={level.index}
                    type="button"
                    onClick={() => changeQuality(String(level.index))}
                  >
                    <span>{level.label}</span>
                    <small>Lock quality</small>
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
      {notice ? <div className="alert">{notice}</div> : null}
      {error ? <div className="alert error">{error}</div> : null}
    </>
  );
}
