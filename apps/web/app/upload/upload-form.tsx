"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { UploadCloud, X } from "lucide-react";
import { abortUpload } from "@/lib/api";
import { uploadFile, type UploadProgress } from "@/lib/uploads";

// Persisted across reloads so an interrupted upload can resume. The browser
// cannot keep the File itself, so we store its identity and ask the user to
// re-select the same file.
const RESUME_KEY = "mediaflow:pending-upload";

type ResumeRecord = {
  sessionId: string;
  title: string;
  fileName: string;
  fileSize: number;
  lastModified: number;
};

function loadResume(): ResumeRecord | null {
  try {
    const raw = localStorage.getItem(RESUME_KEY);
    return raw ? (JSON.parse(raw) as ResumeRecord) : null;
  } catch {
    return null;
  }
}

function saveResume(record: ResumeRecord) {
  try {
    localStorage.setItem(RESUME_KEY, JSON.stringify(record));
  } catch {
    // Best-effort; resume just won't be available.
  }
}

function clearResume() {
  try {
    localStorage.removeItem(RESUME_KEY);
  } catch {
    // ignore
  }
}

function sameFile(record: ResumeRecord, file: File): boolean {
  return (
    record.fileName === file.name &&
    record.fileSize === file.size &&
    record.lastModified === file.lastModified
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(1)} ${units[unit]}`;
}

export function UploadForm() {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [phase, setPhase] = useState<"idle" | "uploading">("idle");
  const [progress, setProgress] = useState<UploadProgress | null>(null);
  const [resume, setResume] = useState<ResumeRecord | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    setResume(loadResume());
  }, []);

  async function startUpload(params: {
    file: File;
    title: string;
    description: string;
    resumeSessionId?: string;
  }) {
    setError(null);
    setPhase("uploading");
    setProgress(null);

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const { videoId } = await uploadFile({
        file: params.file,
        title: params.title,
        description: params.description,
        resumeSessionId: params.resumeSessionId,
        signal: controller.signal,
        onProgress: setProgress,
        onSession: (session) => {
          const record: ResumeRecord = {
            sessionId: session.id,
            title: params.title,
            fileName: params.file.name,
            fileSize: params.file.size,
            lastModified: params.file.lastModified
          };
          saveResume(record);
          setResume(record);
        }
      });
      clearResume();
      router.push(`/videos/${videoId}`);
    } catch (err) {
      if (err instanceof DOMException && err.name === "AbortError") {
        // Cancelled by the user; the session stays resumable.
        setPhase("idle");
        return;
      }
      setError(err instanceof Error ? err.message : "Upload failed.");
      setPhase("idle");
    } finally {
      abortRef.current = null;
    }
  }

  async function onSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);

    const form = event.currentTarget;
    const formData = new FormData(form);
    const title = String(formData.get("title") ?? "").trim();
    const description = String(formData.get("description") ?? "");
    const file = formData.get("file");

    if (!title) {
      setError("Title is required.");
      return;
    }
    if (!(file instanceof File) || file.size === 0) {
      setError("Choose an MP4 file.");
      return;
    }
    if (file.type && file.type !== "video/mp4") {
      setError("Only MP4 uploads are supported.");
      return;
    }

    // Resume if this is the same file the stored session was for; otherwise
    // discard any stale session and start fresh.
    let resumeSessionId: string | undefined;
    if (resume) {
      if (sameFile(resume, file)) {
        resumeSessionId = resume.sessionId;
      } else {
        void abortUpload(resume.sessionId).catch(() => {});
        clearResume();
        setResume(null);
      }
    }

    await startUpload({ file, title, description, resumeSessionId });
  }

  function onCancel() {
    abortRef.current?.abort();
  }

  async function onDiscard() {
    if (resume) {
      await abortUpload(resume.sessionId).catch(() => {});
    }
    clearResume();
    setResume(null);
    setError(null);
  }

  const uploading = phase === "uploading";
  const percent = progress
    ? Math.floor((progress.uploadedBytes / progress.totalBytes) * 100)
    : 0;

  return (
    <form className="form" onSubmit={onSubmit}>
      {resume && !uploading ? (
        <div className="alert">
          Unfinished upload of <strong>{resume.fileName}</strong>. Re-select the
          same file and upload again to resume where it left off, or{" "}
          <button type="button" className="primary-link" onClick={onDiscard}>
            discard it
          </button>
          .
        </div>
      ) : null}

      <div className="field">
        <label htmlFor="title">Title</label>
        <input
          id="title"
          name="title"
          maxLength={140}
          defaultValue={resume?.title ?? ""}
          required
          disabled={uploading}
        />
      </div>
      <div className="field">
        <label htmlFor="description">Description</label>
        <textarea id="description" name="description" disabled={uploading} />
      </div>
      <div className="field">
        <label htmlFor="file">MP4 file</label>
        <input
          id="file"
          name="file"
          type="file"
          accept="video/mp4,.mp4"
          required
          disabled={uploading}
        />
      </div>

      {uploading && progress ? (
        <div>
          <div className="progress">
            <div className="progress-bar" style={{ width: `${percent}%` }} />
          </div>
          <div className="progress-meta">
            <span>
              {formatBytes(progress.uploadedBytes)} of{" "}
              {formatBytes(progress.totalBytes)} ({percent}%)
            </span>
            <span>
              {progress.completedParts}/{progress.totalParts} parts
            </span>
          </div>
        </div>
      ) : null}

      {error ? <div className="alert error">{error}</div> : null}

      <div className="button-row">
        <button className="button" type="submit" disabled={uploading}>
          <UploadCloud size={16} />
          {uploading ? "Uploading" : resume ? "Resume upload" : "Upload"}
        </button>
        {uploading ? (
          <button
            className="button secondary-button"
            type="button"
            onClick={onCancel}
          >
            <X size={16} />
            Cancel
          </button>
        ) : null}
      </div>
    </form>
  );
}
