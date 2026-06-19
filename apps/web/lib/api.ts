export type VideoStatus = "uploading" | "uploaded" | "queued" | "processing" | "ready" | "failed";

export type VideoVariant = {
  quality: string;
  width: number;
  height: number;
  bitrate: number;
  codec?: string;
  playlistKey?: string;
};

export type VideoItem = {
  id: string;
  title: string;
  description: string | null;
  status: VideoStatus;
  thumbnailUrl?: string | null;
  durationSeconds: number | null;
  errorMessage: string | null;
  variants?: VideoVariant[];
  createdAt: string;
  updatedAt: string;
};

export type PlaybackResponse = {
  videoId: string;
  hlsUrl: string;
  expiresAt: string;
};

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8080";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    cache: "no-store"
  });

  if (!response.ok) {
    let message = `Request failed with status ${response.status}`;
    try {
      const body = await response.json();
      message = body?.error?.message ?? message;
    } catch {
      // Keep the status message.
    }
    throw new Error(message);
  }

  return response.json() as Promise<T>;
}

export async function listVideos(): Promise<VideoItem[]> {
  const response = await request<{ items: VideoItem[] }>("/videos");
  return response.items;
}

export async function getVideo(id: string): Promise<VideoItem> {
  return request<VideoItem>(`/videos/${id}`);
}

export async function getPlayback(id: string): Promise<PlaybackResponse> {
  return request<PlaybackResponse>(`/videos/${id}/playback`);
}

export async function uploadVideo(formData: FormData): Promise<VideoItem> {
  return request<VideoItem>("/videos/upload", {
    method: "POST",
    body: formData
  });
}

// --- M6 presigned multipart ingest -----------------------------------------

export type UploadSessionStatus =
  | "pending"
  | "uploading"
  | "completed"
  | "aborted"
  | "expired";

export type UploadedPart = {
  partNumber: number;
  etag: string;
  size: number;
};

export type UploadSession = {
  id: string;
  title: string;
  status: UploadSessionStatus;
  partSize: number;
  totalSize: number;
  partCount: number;
  contentType: string;
  originalFilename: string;
  videoId?: string | null;
  createdAt: string;
  updatedAt: string;
  expiresAt: string;
  uploadedParts?: UploadedPart[];
};

export type CreateUploadSessionInput = {
  title: string;
  description?: string;
  originalFilename: string;
  contentType: string;
  totalSize: number;
  partSize: number;
};

export type PartUrlResponse = {
  partNumber: number;
  url: string;
  method: string;
  expiresAt: string;
};

export type CompleteUploadResponse = {
  videoId: string;
  status: string;
};

export async function createUploadSession(
  input: CreateUploadSessionInput
): Promise<UploadSession> {
  return request<UploadSession>("/uploads", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input)
  });
}

export async function getUploadSession(id: string): Promise<UploadSession> {
  return request<UploadSession>(`/uploads/${id}`);
}

export async function getPartUrl(
  id: string,
  partNumber: number
): Promise<PartUrlResponse> {
  return request<PartUrlResponse>(`/uploads/${id}/parts/${partNumber}/url`);
}

export async function completeUpload(
  id: string,
  parts: { partNumber: number; etag: string }[]
): Promise<CompleteUploadResponse> {
  return request<CompleteUploadResponse>(`/uploads/${id}/complete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ parts })
  });
}

export async function abortUpload(id: string): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/uploads/${id}`, {
    method: "DELETE",
    cache: "no-store"
  });
  if (!response.ok && response.status !== 404) {
    throw new Error(`Failed to abort upload (status ${response.status})`);
  }
}

