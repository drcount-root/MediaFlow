package processor

import (
	"testing"

	"mediaflow/apps/worker/internal/job"
)

func TestPlanRenditionsUsesSourceHeight(t *testing.T) {
	specs := PlanRenditions(720)

	if len(specs) != 3 {
		t.Fatalf("expected 3 renditions, got %d", len(specs))
	}

	if specs[0].Quality != "720p" || specs[1].Quality != "480p" || specs[2].Quality != "360p" {
		t.Fatalf("unexpected renditions: %#v", specs)
	}
}

func TestPlanRenditionsFallsBackForSmallSource(t *testing.T) {
	specs := PlanRenditions(240)

	if len(specs) != 1 {
		t.Fatalf("expected 1 fallback rendition, got %d", len(specs))
	}

	if specs[0].Quality != "360p" {
		t.Fatalf("expected 360p fallback, got %s", specs[0].Quality)
	}
}

func TestBuildMasterPlaylistOrdersByBitrateDescending(t *testing.T) {
	master := string(BuildMasterPlaylist([]job.Variant{
		{Quality: "360p", Width: 640, Height: 360, Bitrate: 800000},
		{Quality: "720p", Width: 1280, Height: 720, Bitrate: 2800000},
		{Quality: "480p", Width: 854, Height: 480, Bitrate: 1400000},
	}))

	want := "#EXTM3U\n#EXT-X-VERSION:3\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=2800000,RESOLUTION=1280x720\n720p/index.m3u8\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=1400000,RESOLUTION=854x480\n480p/index.m3u8\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\n360p/index.m3u8\n"
	if master != want {
		t.Fatalf("master playlist mismatch:\n got: %q\nwant: %q", master, want)
	}
}
