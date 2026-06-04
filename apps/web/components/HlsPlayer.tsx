"use client";

import Hls from "hls.js";
import { Check, Maximize, Minimize, Pause, Play, Settings, Volume2, VolumeX } from "lucide-react";
import type { CSSProperties } from "react";
import { useEffect, useRef, useState } from "react";

type QualityLevel = {
  index: number;
  label: string;
  height: number;
};

export function HlsPlayer({ src }: { src: string }) {
  const frameRef = useRef<HTMLDivElement | null>(null);
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const hlsRef = useRef<Hls | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [levels, setLevels] = useState<QualityLevel[]>([]);
  const [selectedLevel, setSelectedLevel] = useState(-1);
  const [activeLevel, setActiveLevel] = useState<number | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);
  const [isPlaying, setIsPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [volume, setVolume] = useState(1);
  const [isMuted, setIsMuted] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);

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

  useEffect(() => {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    function updatePlaybackState() {
      if (!video) {
        return;
      }
      setIsPlaying(!video.paused);
    }

    function updateTime() {
      if (!video) {
        return;
      }
      setCurrentTime(video.currentTime);
      setDuration(Number.isFinite(video.duration) ? video.duration : 0);
    }

    function updateVolume() {
      if (!video) {
        return;
      }
      setVolume(video.volume);
      setIsMuted(video.muted || video.volume === 0);
    }

    video.addEventListener("play", updatePlaybackState);
    video.addEventListener("pause", updatePlaybackState);
    video.addEventListener("timeupdate", updateTime);
    video.addEventListener("durationchange", updateTime);
    video.addEventListener("loadedmetadata", updateTime);
    video.addEventListener("volumechange", updateVolume);
    updatePlaybackState();
    updateTime();
    updateVolume();

    return () => {
      video.removeEventListener("play", updatePlaybackState);
      video.removeEventListener("pause", updatePlaybackState);
      video.removeEventListener("timeupdate", updateTime);
      video.removeEventListener("durationchange", updateTime);
      video.removeEventListener("loadedmetadata", updateTime);
      video.removeEventListener("volumechange", updateVolume);
    };
  }, []);

  useEffect(() => {
    function updateFullscreen() {
      setIsFullscreen(document.fullscreenElement === frameRef.current);
    }

    document.addEventListener("fullscreenchange", updateFullscreen);
    return () => {
      document.removeEventListener("fullscreenchange", updateFullscreen);
    };
  }, []);

  function formatTime(value: number) {
    if (!Number.isFinite(value) || value <= 0) {
      return "0:00";
    }

    const totalSeconds = Math.floor(value);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}:${seconds.toString().padStart(2, "0")}`;
  }

  function togglePlayback() {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    if (video.paused) {
      void video.play();
    } else {
      video.pause();
    }
  }

  function seek(value: string) {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    const nextTime = Number(value);
    video.currentTime = nextTime;
    setCurrentTime(nextTime);
  }

  function changeVolume(value: string) {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    const nextVolume = Number(value);
    video.volume = nextVolume;
    video.muted = nextVolume === 0;
    setVolume(nextVolume);
    setIsMuted(video.muted);
  }

  function toggleMute() {
    const video = videoRef.current;
    if (!video) {
      return;
    }

    if (video.muted || video.volume === 0) {
      video.muted = false;
      if (video.volume === 0) {
        video.volume = 0.7;
      }
    } else {
      video.muted = true;
    }
  }

  function toggleFullscreen() {
    const frame = frameRef.current;
    if (!frame) {
      return;
    }

    if (document.fullscreenElement) {
      void document.exitFullscreen();
    } else {
      void frame.requestFullscreen();
    }
  }

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
  const qualityStatus = selectedLevel === -1 ? `Auto ${activeLabel === "detecting" ? "" : activeLabel}`.trim() : selectedLabel;
  const progress = duration > 0 ? (currentTime / duration) * 100 : 0;

  return (
    <>
      <div className="player-frame" ref={frameRef}>
        <video ref={videoRef} className="player" playsInline onClick={togglePlayback} />
        <button className="player-center-action" type="button" onClick={togglePlayback} aria-label={isPlaying ? "Pause video" : "Play video"}>
          {isPlaying ? <Pause size={34} fill="currentColor" /> : <Play size={34} fill="currentColor" />}
        </button>
        <div className="player-controls">
          <div className="player-progress-row">
            <input
              aria-label="Seek video"
              className="player-progress"
              max={duration || 0}
              min="0"
              onChange={(event) => seek(event.target.value)}
              step="0.1"
              style={{ "--progress": `${progress}%` } as CSSProperties}
              type="range"
              value={Math.min(currentTime, duration || currentTime)}
            />
          </div>
          <div className="player-control-row">
            <button className="player-icon-button" type="button" onClick={togglePlayback} aria-label={isPlaying ? "Pause video" : "Play video"}>
              {isPlaying ? <Pause size={20} fill="currentColor" /> : <Play size={20} fill="currentColor" />}
            </button>
            <span className="player-time">
              {formatTime(currentTime)} / {formatTime(duration)}
            </span>
            <div className="player-spacer" />
            <div className="player-volume">
              <button className="player-icon-button" type="button" onClick={toggleMute} aria-label={isMuted ? "Unmute video" : "Mute video"}>
                {isMuted ? <VolumeX size={20} /> : <Volume2 size={20} />}
              </button>
              <input
                aria-label="Volume"
                className="volume-slider"
                max="1"
                min="0"
                onChange={(event) => changeVolume(event.target.value)}
                step="0.05"
                type="range"
                value={isMuted ? 0 : volume}
              />
            </div>
            {levels.length > 0 ? (
              <div className="quality-menu">
                <button className="quality-trigger" type="button" onClick={() => setMenuOpen((open) => !open)} aria-expanded={menuOpen}>
                  <Settings size={18} />
                  <span>{qualityStatus}</span>
                </button>
                {menuOpen ? (
                  <div className="quality-popover" role="menu" aria-label="Video quality">
                    <div className="quality-heading">Quality</div>
                    <button className={selectedLevel === -1 ? "quality-option active" : "quality-option"} type="button" onClick={() => changeQuality("-1")}>
                      <span>Auto</span>
                      <small>{activeLevel === null ? "detecting" : `currently ${activeLabel}`}</small>
                      {selectedLevel === -1 ? <Check size={16} /> : null}
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
                        <small>Manual</small>
                        {selectedLevel === level.index ? <Check size={16} /> : null}
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : null}
            <button className="player-icon-button" type="button" onClick={toggleFullscreen} aria-label={isFullscreen ? "Exit fullscreen" : "Enter fullscreen"}>
              {isFullscreen ? <Minimize size={20} /> : <Maximize size={20} />}
            </button>
          </div>
        </div>
        {!isPlaying ? (
          <button className="player-poster-action" type="button" onClick={togglePlayback} aria-label="Play video">
            <span>
              <Play size={24} fill="currentColor" />
            </span>
          </button>
        ) : null}
      </div>
      {notice ? <div className="alert">{notice}</div> : null}
      {error ? <div className="alert error">{error}</div> : null}
    </>
  );
}
