"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { UploadCloud } from "lucide-react";
import { uploadVideo } from "@/lib/api";

export function UploadForm() {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function onSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);

    const form = event.currentTarget;
    const formData = new FormData(form);
    const title = String(formData.get("title") ?? "").trim();
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

    setSubmitting(true);
    try {
      const video = await uploadVideo(formData);
      router.push(`/videos/${video.id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form className="form" onSubmit={onSubmit}>
      <div className="field">
        <label htmlFor="title">Title</label>
        <input id="title" name="title" maxLength={140} required />
      </div>
      <div className="field">
        <label htmlFor="description">Description</label>
        <textarea id="description" name="description" />
      </div>
      <div className="field">
        <label htmlFor="file">MP4 file</label>
        <input id="file" name="file" type="file" accept="video/mp4,.mp4" required />
      </div>
      {error ? <div className="alert error">{error}</div> : null}
      <button className="button" type="submit" disabled={submitting}>
        <UploadCloud size={16} />
        {submitting ? "Uploading" : "Upload"}
      </button>
    </form>
  );
}

