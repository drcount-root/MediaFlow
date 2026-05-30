package processor

import "testing"

func TestSelectVariantsUsesSourceHeight(t *testing.T) {
	variants := selectVariants(720)

	if len(variants) != 3 {
		t.Fatalf("expected 3 variants, got %d", len(variants))
	}

	if variants[0].Quality != "720p" || variants[1].Quality != "480p" || variants[2].Quality != "360p" {
		t.Fatalf("unexpected variants: %#v", variants)
	}
}

func TestSelectVariantsFallsBackForSmallSource(t *testing.T) {
	variants := selectVariants(240)

	if len(variants) != 1 {
		t.Fatalf("expected 1 fallback variant, got %d", len(variants))
	}

	if variants[0].Quality != "360p" {
		t.Fatalf("expected 360p fallback, got %s", variants[0].Quality)
	}
}
