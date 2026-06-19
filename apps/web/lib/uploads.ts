// Chunked, resumable upload engine for the M6 presigned multipart ingest.
//
// Bytes never transit the API: we create a session, then PUT each part slice
// directly to the presigned object-storage URL. Parts upload with bounded
// concurrency and per-part retry; an interrupted upload resumes from the parts
// object storage already holds (GET /uploads/:id returns them).

import {
  completeUpload,
  createUploadSession,
  getPartUrl,
  getUploadSession,
  type UploadSession
} from "@/lib/api";

const MiB = 1024 * 1024;
const DEFAULT_PART_SIZE = 8 * MiB;
const MAX_PART_COUNT = 10000; // S3/MinIO multipart hard limit
const DEFAULT_CONCURRENCY = 4;
const MAX_PART_ATTEMPTS = 4;

export type UploadProgress = {
  uploadedBytes: number;
  totalBytes: number;
  completedParts: number;
  totalParts: number;
};

export type UploadFileOptions = {
  file: File;
  title: string;
  description?: string;
  // When set, resume an existing session instead of creating a new one.
  resumeSessionId?: string;
  concurrency?: number;
  signal?: AbortSignal;
  onProgress?: (progress: UploadProgress) => void;
  // Called once the session is known (after create or resume) so the caller can
  // persist its id for resume-after-reload.
  onSession?: (session: UploadSession) => void;
};

export type UploadResult = {
  videoId: string;
  sessionId: string;
};

// choosePartSize keeps the part count under the multipart limit: 8 MiB parts by
// default, bumped up (rounded to whole MiB) only for very large files.
export function choosePartSize(totalSize: number): number {
  const minForCount = Math.ceil(totalSize / MAX_PART_COUNT);
  if (minForCount <= DEFAULT_PART_SIZE) {
    return DEFAULT_PART_SIZE;
  }
  return Math.ceil(minForCount / MiB) * MiB;
}

export async function uploadFile(opts: UploadFileOptions): Promise<UploadResult> {
  const { file, signal, onProgress, onSession } = opts;
  const concurrency = opts.concurrency ?? DEFAULT_CONCURRENCY;

  let session: UploadSession;
  if (opts.resumeSessionId) {
    session = await getUploadSession(opts.resumeSessionId);
    if (session.status === "completed" && session.videoId) {
      return { videoId: session.videoId, sessionId: session.id };
    }
    if (session.status !== "pending" && session.status !== "uploading") {
      throw new Error("This upload can no longer be resumed; please start over.");
    }
    if (session.totalSize !== file.size) {
      throw new Error("The selected file does not match the upload being resumed.");
    }
  } else {
    session = await createUploadSession({
      title: opts.title,
      description: opts.description,
      originalFilename: file.name,
      contentType: file.type || "video/mp4",
      totalSize: file.size,
      partSize: choosePartSize(file.size)
    });
  }
  onSession?.(session);

  const { partSize, partCount } = session;
  const sizeOfPart = (n: number) =>
    n < partCount ? partSize : file.size - partSize * (partCount - 1);

  // Parts object storage already holds (resume): partNumber -> etag.
  const doneEtags = new Map<number, string>();
  for (const p of session.uploadedParts ?? []) {
    doneEtags.set(p.partNumber, p.etag);
  }

  // Per-part uploaded-byte counters drive a smooth aggregate progress bar.
  const partUploaded = new Array<number>(partCount + 1).fill(0);
  for (const n of doneEtags.keys()) {
    partUploaded[n] = sizeOfPart(n);
  }

  const emit = () => {
    if (!onProgress) return;
    let uploaded = 0;
    for (let n = 1; n <= partCount; n++) uploaded += partUploaded[n];
    onProgress({
      uploadedBytes: uploaded,
      totalBytes: file.size,
      completedParts: doneEtags.size,
      totalParts: partCount
    });
  };
  emit();

  const pending: number[] = [];
  for (let n = 1; n <= partCount; n++) {
    if (!doneEtags.has(n)) pending.push(n);
  }

  // An internal controller lets one failed part abort its siblings' in-flight
  // PUTs, and chains to the caller's signal (cancel button / unmount).
  const controller = new AbortController();
  const onExternalAbort = () => controller.abort();
  if (signal) {
    if (signal.aborted) controller.abort();
    else signal.addEventListener("abort", onExternalAbort, { once: true });
  }

  let cursor = 0;
  const worker = async () => {
    while (cursor < pending.length) {
      if (controller.signal.aborted) throw new DOMException("Aborted", "AbortError");
      const n = pending[cursor++];
      const start = partSize * (n - 1);
      const blob = file.slice(start, start + sizeOfPart(n));
      const etag = await uploadPartWithRetry(
        session.id,
        n,
        blob,
        controller.signal,
        (loaded) => {
          partUploaded[n] = loaded;
          emit();
        }
      );
      partUploaded[n] = sizeOfPart(n);
      doneEtags.set(n, etag);
      emit();
    }
  };

  try {
    const lanes = Math.min(concurrency, pending.length);
    await Promise.all(Array.from({ length: lanes }, () => worker()));
  } catch (err) {
    controller.abort(); // stop the other lanes
    throw err;
  } finally {
    signal?.removeEventListener("abort", onExternalAbort);
  }

  const parts = Array.from(doneEtags.entries())
    .map(([partNumber, etag]) => ({ partNumber, etag }))
    .sort((a, b) => a.partNumber - b.partNumber);

  const result = await completeUpload(session.id, parts);
  return { videoId: result.videoId, sessionId: session.id };
}

async function uploadPartWithRetry(
  sessionId: string,
  partNumber: number,
  blob: Blob,
  signal: AbortSignal,
  onProgress: (loaded: number) => void
): Promise<string> {
  let lastErr: unknown;
  for (let attempt = 1; attempt <= MAX_PART_ATTEMPTS; attempt++) {
    if (signal.aborted) throw new DOMException("Aborted", "AbortError");
    try {
      const { url } = await getPartUrl(sessionId, partNumber);
      return await putPart(url, blob, signal, onProgress);
    } catch (err) {
      if (isAbort(err)) throw err;
      lastErr = err;
      onProgress(0); // this part made no durable progress; reset its counter
      if (attempt < MAX_PART_ATTEMPTS) {
        await delay(backoffMs(attempt), signal);
      }
    }
  }
  throw lastErr instanceof Error
    ? lastErr
    : new Error(`Part ${partNumber} failed after ${MAX_PART_ATTEMPTS} attempts.`);
}

// putPart uses XMLHttpRequest (not fetch) because only XHR exposes upload
// progress events. The ETag comes back in the response header — object storage
// must expose it via CORS (MinIO does by default).
function putPart(
  url: string,
  blob: Blob,
  signal: AbortSignal,
  onProgress: (loaded: number) => void
): Promise<string> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url);
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress(event.loaded);
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        const etag = xhr.getResponseHeader("ETag");
        if (!etag) {
          reject(new Error("Upload response was missing its ETag header."));
          return;
        }
        resolve(etag);
      } else {
        reject(new Error(`Part upload failed with status ${xhr.status}.`));
      }
    };
    xhr.onerror = () => reject(new Error("Network error during part upload."));
    xhr.onabort = () => reject(new DOMException("Aborted", "AbortError"));

    const abort = () => xhr.abort();
    if (signal.aborted) {
      xhr.abort();
      return;
    }
    signal.addEventListener("abort", abort, { once: true });
    xhr.onloadend = () => signal.removeEventListener("abort", abort);

    xhr.send(blob);
  });
}

function backoffMs(attempt: number): number {
  const base = Math.min(8000, 300 * 2 ** (attempt - 1));
  return base + Math.floor(Math.random() * 250); // jitter
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true }
    );
  });
}

function isAbort(err: unknown): boolean {
  return err instanceof DOMException && err.name === "AbortError";
}
