package dependencies_test

import (
	"go/version"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestHTTP2SecurityBaseline prevents reintroducing the advisories that broke
// CI. Check declarations so this regression also runs offline without
// downloading the module graph. CI's govulncheck detects new advisories.
func TestHTTP2SecurityBaseline(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	var goVersion string
	modules := make(map[string]string)
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "go":
			goVersion = "go" + fields[1]
		default:
			if strings.HasPrefix(fields[1], "v") {
				modules[fields[0]] = fields[1]
			}
		}
	}
	t.Run("Go", func(t *testing.T) {
		if !version.IsValid(goVersion) || version.Compare(goVersion, "go1.26.10") < 0 ||
			(version.Compare(goVersion, "go1.27") >= 0 && version.Compare(goVersion, "go1.27.3") < 0) {
			t.Errorf("Go %q fails the GO-2026-6617/GO-2026-6809 baseline; require Go 1.26.10 or a later fixed release", goVersion)
		}
	})
	// These minimums fix GO-2026-6617 (x/net), GO-2026-6348,
	// GO-2026-6441 and GO-2026-6443 (grpc), and GO-2026-6505 (OTel).
	minimums := map[string]string{
		"golang.org/x/net":                                                "v0.60.0",
		"google.golang.org/grpc":                                          "v1.83.2",
		"go.opentelemetry.io/otel":                                        "v1.45.0",
		"go.opentelemetry.io/otel/trace":                                  "v1.45.0",
		"go.opentelemetry.io/otel/metric":                                 "v1.45.0",
		"go.opentelemetry.io/otel/sdk":                                    "v1.45.0",
		"go.opentelemetry.io/otel/exporters/otlp/otlptrace":               "v1.45.0",
		"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp": "v1.45.0",
	}
	for module, minimum := range minimums {
		t.Run(module, func(t *testing.T) {
			selected := stableRelease(t, modules[module])
			floor := stableRelease(t, minimum)
			if slices.Compare(selected[:], floor[:]) < 0 {
				t.Errorf("%s %s fails the security baseline; require %s or later", module, modules[module], minimum)
			}
		})
	}
}

func stableRelease(t *testing.T, v string) [3]int {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(v, "v"), ".")
	if len(parts) != 3 || !strings.HasPrefix(v, "v") {
		t.Fatalf("require a stable module release, got %q", v)
	}
	var result [3]int
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			t.Fatalf("require a stable module release, got %q", v)
		}
		result[i] = n
	}
	return result
}
