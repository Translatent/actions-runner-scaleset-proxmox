package dependencies_test

import (
	"go/version"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestHTTP2SecurityBaseline prevents reintroducing GO-2026-6617 through
// either x/net/http2 or the standard library. Check declarations so this
// regression also runs offline without downloading the module graph.
func TestHTTP2SecurityBaseline(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	var goVersion, netVersion string
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "go":
			goVersion = "go" + fields[1]
		case "golang.org/x/net":
			netVersion = fields[1]
		}
	}
	t.Run("Go", func(t *testing.T) {
		if !version.IsValid(goVersion) || version.Compare(goVersion, "go1.26.9") < 0 ||
			(version.Compare(goVersion, "go1.27") >= 0 && version.Compare(goVersion, "go1.27.2") < 0) {
			t.Errorf("Go %q is affected by GO-2026-6617; require Go 1.26.9 or a later fixed release", goVersion)
		}
	})
	t.Run("x/net", func(t *testing.T) {
		parts := strings.Split(strings.TrimPrefix(netVersion, "v"), ".")
		if len(parts) != 3 || !strings.HasPrefix(netVersion, "v") {
			t.Fatalf("require a stable x/net release, got %q", netVersion)
		}
		var selected [3]int
		for i, part := range parts {
			n, err := strconv.Atoi(part)
			if err != nil || n < 0 {
				t.Fatalf("require a stable x/net release, got %q", netVersion)
			}
			selected[i] = n
		}
		if slices.Compare(selected[:], []int{0, 60, 0}) < 0 {
			t.Errorf("x/net %s is affected by GO-2026-6617; require v0.60.0 or later", netVersion)
		}
	})
}
