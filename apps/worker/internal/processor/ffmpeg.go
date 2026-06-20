package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

// GenerateRendition transcodes the source into exactly one quality (M7). It
// writes index.m3u8 + segment_*.ts into outputDir and returns the variant with
// its local dir populated; the caller uploads the dir and records the variant.
func (p FFmpegProcessor) GenerateRendition(ctx context.Context, inputPath, outputDir string, spec job.RenditionSpec, hasAudio bool) (job.Variant, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return job.Variant{}, err
	}

	playlist := filepath.Join(outputDir, "index.m3u8")
	args := []string{
		"-y",
		"-i", inputPath,
		"-vf", fmt.Sprintf("scale=-2:%d", spec.Height),
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-b:v", fmt.Sprintf("%dk", spec.Bitrate/1000),
		"-maxrate", fmt.Sprintf("%dk", int(float64(spec.Bitrate)*1.07)/1000),
		"-bufsize", fmt.Sprintf("%dk", int(float64(spec.Bitrate)*1.5)/1000),
		"-g", "48",
		"-keyint_min", "48",
		"-sc_threshold", "0",
	}

	if hasAudio {
		args = append(args, "-c:a", "aac", "-ar", "48000", "-b:a", "128k")
	} else {
		args = append(args, "-an")
	}

	args = append(args,
		"-hls_time", "4",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", filepath.Join(outputDir, "segment_%03d.ts"),
		playlist,
	)

	cmd := exec.CommandContext(ctx, p.FFmpegPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return job.Variant{}, fmt.Errorf("hls ffmpeg failed for %s: %w: %s", spec.Quality, err, string(output))
	}

	return job.Variant{
		Quality:     spec.Quality,
		Width:       spec.Width,
		Height:      spec.Height,
		Bitrate:     spec.Bitrate,
		Codec:       spec.Codec,
		PlaylistKey: "index.m3u8",
		LocalDir:    outputDir,
	}, nil
}

// BuildMasterPlaylist renders a master.m3u8 referencing each variant's
// {quality}/index.m3u8. Variants are written in descending bitrate so players
// pick a sensible default. The finalize stage uploads the result (M7).
func BuildMasterPlaylist(variants []job.Variant) []byte {
	ordered := append([]job.Variant(nil), variants...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Bitrate > ordered[j].Bitrate })

	master := &bytes.Buffer{}
	master.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	for _, v := range ordered {
		master.WriteString(fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n%s/index.m3u8\n", v.Bitrate, v.Width, v.Height, v.Quality))
	}
	return master.Bytes()
}

// PlanRenditions picks the target qualities for a source of the given height.
// It is the map step of the fan-out: the planner turns this list into one
// rendition job per spec. A tiny source still gets the smallest rendition.
func PlanRenditions(sourceHeight int) []job.RenditionSpec {
	candidates := []job.RenditionSpec{
		{Quality: "720p", Width: 1280, Height: 720, Bitrate: 2800000, Codec: "h264"},
		{Quality: "480p", Width: 854, Height: 480, Bitrate: 1400000, Codec: "h264"},
		{Quality: "360p", Width: 640, Height: 360, Bitrate: 800000, Codec: "h264"},
	}

	var selected []job.RenditionSpec
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
