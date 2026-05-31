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

