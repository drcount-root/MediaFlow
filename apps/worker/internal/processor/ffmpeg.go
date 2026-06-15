package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"mediaflow/apps/worker/internal/job"
)

type FFmpegProcessor struct {
	FFmpegPath  string
	FFprobePath string
}

func (p FFmpegProcessor) Probe(ctx context.Context, inputPath string) (job.ProbeResult, error) {
	cmd := exec.CommandContext(ctx, p.FFprobePath, "-v", "error", "-show_entries", "format=duration", "-show_entries", "stream=codec_type,width,height", "-of", "json", inputPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// ffprobe rejecting the input means it is unreadable/corrupt — retrying the
		// same bytes will not help, so this is a permanent failure.
		return job.ProbeResult{}, job.Permanent(fmt.Errorf("ffprobe failed: %w: %s", err, string(output)))
	}

	var parsed struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			CodecType string `json:"codec_type"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(output, &parsed); err != nil {
		return job.ProbeResult{}, err
	}

	duration, _ := strconv.ParseFloat(parsed.Format.Duration, 64)
	result := job.ProbeResult{DurationSeconds: duration}
	for _, stream := range parsed.Streams {
		switch stream.CodecType {
		case "video":
			if result.Width == 0 {
				result.Width = stream.Width
				result.Height = stream.Height
			}
		case "audio":
			result.HasAudio = true
		}
	}

	if result.Width == 0 || result.Height == 0 {
		return job.ProbeResult{}, job.Permanent(fmt.Errorf("no video stream found"))
	}

	return result, nil
}

func (p FFmpegProcessor) GenerateThumbnail(ctx context.Context, inputPath, outputPath string) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, p.FFmpegPath, "-y", "-ss", "00:00:01", "-i", inputPath, "-frames:v", "1", "-vf", "scale=640:-1", outputPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("thumbnail ffmpeg failed: %w: %s", err, string(output))
	}
	return nil
}

func (p FFmpegProcessor) GenerateHLS(ctx context.Context, inputPath, outputDir string, probe job.ProbeResult) ([]job.Variant, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, err
	}

	variants := selectVariants(probe.Height)
	if len(variants) == 0 {
		return nil, fmt.Errorf("no variants selected for source height %d", probe.Height)
	}

	master := &bytes.Buffer{}
	master.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")

	for idx := range variants {
		variant := variants[idx]
		localDir := filepath.Join(outputDir, variant.Quality)
		if err := os.MkdirAll(localDir, 0o755); err != nil {
			return nil, err
		}

		playlist := filepath.Join(localDir, "index.m3u8")
		args := []string{
			"-y",
			"-i", inputPath,
			"-vf", fmt.Sprintf("scale=-2:%d", variant.Height),
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-b:v", fmt.Sprintf("%dk", variant.Bitrate/1000),
			"-maxrate", fmt.Sprintf("%dk", int(float64(variant.Bitrate)*1.07)/1000),
			"-bufsize", fmt.Sprintf("%dk", int(float64(variant.Bitrate)*1.5)/1000),
			"-g", "48",
			"-keyint_min", "48",
			"-sc_threshold", "0",
		}

		if probe.HasAudio {
			args = append(args, "-c:a", "aac", "-ar", "48000", "-b:a", "128k")
		} else {
			args = append(args, "-an")
		}

		args = append(args,
			"-hls_time", "4",
			"-hls_playlist_type", "vod",
			"-hls_segment_filename", filepath.Join(localDir, "segment_%03d.ts"),
			playlist,
		)

		cmd := exec.CommandContext(ctx, p.FFmpegPath, args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("hls ffmpeg failed for %s: %w: %s", variant.Quality, err, string(output))
		}

		variants[idx].LocalDir = localDir
		variants[idx].PlaylistKey = "index.m3u8"
		master.WriteString(fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n%s/index.m3u8\n", variant.Bitrate, variant.Width, variant.Height, variant.Quality))
	}

	if err := os.WriteFile(filepath.Join(outputDir, "master.m3u8"), master.Bytes(), 0o644); err != nil {
		return nil, err
	}

	return variants, nil
}

func selectVariants(sourceHeight int) []job.Variant {
	candidates := []job.Variant{
		{Quality: "720p", Width: 1280, Height: 720, Bitrate: 2800000, Codec: "h264"},
		{Quality: "480p", Width: 854, Height: 480, Bitrate: 1400000, Codec: "h264"},
		{Quality: "360p", Width: 640, Height: 360, Bitrate: 800000, Codec: "h264"},
	}

	var selected []job.Variant
	for _, candidate := range candidates {
		if sourceHeight >= candidate.Height {
			selected = append(selected, candidate)
		}
	}
	if len(selected) == 0 {
		selected = append(selected, candidates[len(candidates)-1])
	}
	return selected
}

func ContentType(path string) string {
	if strings.HasSuffix(path, ".m3u8") {
		return "application/vnd.apple.mpegurl"
	}
	if strings.HasSuffix(path, ".ts") {
		return "video/mp2t"
	}
	return "application/octet-stream"
}
