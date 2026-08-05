//go:build e2e

package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/testutil/fakegithub"
)

func TestE2E_JITUnauthorizedRefreshRecoversWithoutRestart(t *testing.T) {
	t.Parallel()
	gh := fakegithub.New(t, fakegithub.Options{
		ScaleSet: fakegithub.ScaleSetOptions{Name: "jit-refresh-set"},
	})
	gh.InjectGenerateJITFailure(http.StatusUnauthorized, 1)
	// One initial demand signal is the entire stimulus. The app starts once;
	// the test never posts a second desired-count message.
	gh.SetStatistics(fakegithub.Statistics{TotalAssignedJobs: 1})

	h := Start(t, Options{
		HotSize:              1,
		MaxConcurrentRunners: 1,
		ScaleSetName:         "jit-refresh-set",
		FakeGitHub:           gh,
	})
	assigned := awaitNAssignedVMs(t, h, "jit-refresh-set", 1)
	require.Len(t, assigned, 1)
	require.Equal(t, 2, gh.JITAttemptCount(), "one rejected request plus one identical retry")
	require.Equal(t, 1, gh.JITMintCount(), "refresh must not leak a duplicate runner registration")
	attempts := gh.JITAttempts()
	require.Equal(t, attempts[0], attempts[1], "retry must preserve runner name and work folder")
	require.Equal(t, 1.0, h.MetricValue(t, "scaleset_jit_token_refresh_total",
		formatLabel("scaleset", "jit-refresh-set"), formatLabel("outcome", "success")))
	require.Equal(t, 0.0, h.MetricValue(t, "scaleset_jit_token_refresh_total",
		formatLabel("scaleset", "jit-refresh-set"), formatLabel("outcome", "failure")))

	resp, err := http.Get(h.ObsURL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	metrics := string(body)
	require.True(t, strings.Contains(metrics, `scaleset_jit_token_refresh_total{outcome="success",scaleset="jit-refresh-set"} 1`))
	require.True(t, strings.Contains(metrics, `scaleset_jit_token_refresh_total{outcome="failure",scaleset="jit-refresh-set"} 0`),
		"the bounded failure series must be exported before its first incident")
}
