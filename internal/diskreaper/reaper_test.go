package diskreaper

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
)

func TestEvaluateFourConditions(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	base := Inventory{
		Ranges:       []config.VMIDRange{{Min: 900, Max: 999}},
		GuestConfigs: map[int]struct{}{}, ReplicationJobs: map[int]struct{}{},
		Now: now, MinimumAge: time.Hour,
	}
	oldLeak := Candidate{VMID: 905, Dataset: "vmdata:vm-905-disk-0", CreatedAt: now.Add(-2 * time.Hour)}
	require.True(t, Evaluate(oldLeak, base).Eligible, "a genuine leak must be accepted")

	tests := []struct {
		name      string
		candidate Candidate
		mutate    func(*Inventory)
	}{
		{name: "outside configured range", candidate: Candidate{VMID: 100, Dataset: "vmdata:vm-100-disk-0", CreatedAt: oldLeak.CreatedAt}},
		{name: "guest config on another node", candidate: oldLeak, mutate: func(i *Inventory) { i.GuestConfigs[905] = struct{}{} }},
		{name: "replication job", candidate: oldLeak, mutate: func(i *Inventory) { i.ReplicationJobs[905] = struct{}{} }},
		{name: "younger than threshold", candidate: Candidate{VMID: 905, Dataset: oldLeak.Dataset, CreatedAt: now.Add(-59 * time.Minute)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inv := Inventory{Ranges: append([]config.VMIDRange(nil), base.Ranges...),
				GuestConfigs: map[int]struct{}{}, ReplicationJobs: map[int]struct{}{},
				Now: base.Now, MinimumAge: base.MinimumAge}
			if tc.mutate != nil {
				tc.mutate(&inv)
			}
			require.False(t, Evaluate(tc.candidate, inv).Eligible)
		})
	}
}

func TestEvaluateUnknownAgeFailsClosed(t *testing.T) {
	d := Evaluate(Candidate{VMID: 905}, Inventory{
		Ranges:       []config.VMIDRange{{Min: 900, Max: 999}},
		GuestConfigs: map[int]struct{}{}, ReplicationJobs: map[int]struct{}{},
		Now: time.Now(), MinimumAge: time.Hour,
	})
	require.False(t, d.OldEnough)
	require.False(t, d.Eligible)
}

func TestEvaluatePreservedInitialRefusesDeletion(t *testing.T) {
	now := time.Now()
	candidate := Candidate{VMID: 905, Node: "pve2", Dataset: "vmdata:vm-905-disk-0", CreatedAt: now.Add(-2 * time.Hour)}
	d := Evaluate(candidate, Inventory{
		Ranges: []config.VMIDRange{{Min: 900, Max: 999}}, GuestConfigs: map[int]struct{}{},
		ReplicationJobs: map[int]struct{}{}, Now: now, MinimumAge: time.Hour,
		PreservedInitial: map[string]struct{}{candidateKey(candidate): {}},
	})
	require.True(t, d.InConfiguredRange)
	require.True(t, d.GuestConfigAbsent)
	require.True(t, d.ReplicationJobAbsent)
	require.True(t, d.OldEnough)
	require.True(t, d.PreservedInitial)
	require.False(t, d.Eligible)
}

func TestInitializeBaselinePreservesOnlyInitialEligibleSet(t *testing.T) {
	now := time.Now()
	r := &Reaper{
		cfg:   Config{PreserveInitial: true, Ranges: []config.VMIDRange{{Min: 900, Max: 999}}},
		state: &persistedState{Observations: map[string]time.Time{}, PreservedInitial: map[string]struct{}{}},
	}
	initial := Candidate{VMID: 905, Node: "pve2", Dataset: "vmdata:vm-905-disk-0", CreatedAt: now}
	configured := Candidate{VMID: 906, Node: "pve2", Dataset: "vmdata:vm-906-disk-0", CreatedAt: now}
	r.initializeBaseline([]Candidate{initial, configured}, map[int]struct{}{906: {}}, map[int]struct{}{}, now)
	require.Contains(t, r.state.PreservedInitial, candidateKey(initial))
	require.NotContains(t, r.state.PreservedInitial, candidateKey(configured))

	later := Candidate{VMID: 907, Node: "pve2", Dataset: "vmdata:vm-907-disk-0", CreatedAt: now}
	r.initializeBaseline([]Candidate{later}, map[int]struct{}{}, map[int]struct{}{}, now.Add(time.Hour))
	require.NotContains(t, r.state.PreservedInitial, candidateKey(later),
		"a later orphan must remain automatic after state initialization")
}

func TestReleaseReusedBaselineMakesLaterSameNameAutomatic(t *testing.T) {
	now := time.Now()
	reused := Candidate{VMID: 905, Node: "pve2", Dataset: "vmdata:vm-905-disk-0", CreatedAt: now.Add(-2 * time.Hour)}
	other := Candidate{VMID: 906, Node: "pve2", Dataset: "vmdata:vm-906-disk-0", CreatedAt: now.Add(-2 * time.Hour)}
	r := &Reaper{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		state: &persistedState{PreservedInitial: map[string]struct{}{
			candidateKey(reused): {},
			candidateKey(other):  {},
		}},
	}

	r.releaseReusedBaseline(map[int]struct{}{905: {}})

	require.NotContains(t, r.state.PreservedInitial, candidateKey(reused))
	require.Contains(t, r.state.PreservedInitial, candidateKey(other))
	decision := Evaluate(reused, Inventory{
		Ranges: []config.VMIDRange{{Min: 900, Max: 999}}, GuestConfigs: map[int]struct{}{},
		ReplicationJobs: map[int]struct{}{}, Now: now, MinimumAge: time.Hour,
		PreservedInitial: r.state.PreservedInitial,
	})
	require.True(t, decision.Eligible,
		"a later orphan reusing the same volume name must not inherit cutover protection")
}
