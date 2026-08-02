// Package diskreaper removes Proxmox image volumes left behind before a VM
// config was committed. It fails closed: incomplete inventory never deletes.
package diskreaper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/luthermonson/go-proxmox"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
)

var diskVolumeRE = regexp.MustCompile(`(?:^|:)vm-([0-9]+)-disk-[0-9]+$`)

// Candidate is one storage volume that has the runner disk naming shape.
type Candidate struct {
	VMID      int
	Node      string
	Dataset   string
	Size      uint64
	CreatedAt time.Time
}

// Inventory is the complete safety input used to evaluate a candidate.
type Inventory struct {
	Ranges          []config.VMIDRange
	GuestConfigs    map[int]struct{}
	ReplicationJobs map[int]struct{}
	Now             time.Time
	MinimumAge      time.Duration
}

// Decision records all four load-bearing conditions. Eligible is true only
// when every condition is true.
type Decision struct {
	Candidate            Candidate
	InConfiguredRange    bool
	GuestConfigAbsent    bool
	ReplicationJobAbsent bool
	OldEnough            bool
	Age                  time.Duration
	Eligible             bool
}

// Evaluate applies the four-condition deletion gate without side effects.
func Evaluate(candidate Candidate, inventory Inventory) Decision {
	d := Decision{Candidate: candidate}
	for _, r := range inventory.Ranges {
		if candidate.VMID >= r.Min && candidate.VMID <= r.Max {
			d.InConfiguredRange = true
			break
		}
	}
	_, hasConfig := inventory.GuestConfigs[candidate.VMID]
	d.GuestConfigAbsent = !hasConfig
	_, hasReplication := inventory.ReplicationJobs[candidate.VMID]
	d.ReplicationJobAbsent = !hasReplication
	if !candidate.CreatedAt.IsZero() {
		d.Age = inventory.Now.Sub(candidate.CreatedAt)
		d.OldEnough = d.Age >= inventory.MinimumAge
	}
	d.Eligible = d.InConfiguredRange && d.GuestConfigAbsent && d.ReplicationJobAbsent && d.OldEnough
	return d
}

// Config controls one periodic reaper. StateFile persists the first complete
// observation of volumes whose storage backend does not expose ctime (notably
// PVE ZFS image content). Seeing the same volume after MinimumAge proves the
// dataset itself is at least that old without guessing from its name.
type Config struct {
	Storage    string
	Ranges     []config.VMIDRange
	Interval   time.Duration
	MinimumAge time.Duration
	StateFile  string
	DryRun     bool
	Now        func() time.Time
}

// Result is the stable output of one sweep.
type Result struct {
	Decisions []Decision
	Eligible  []Decision
}

// Reaper inventories through the PVE API and deletes only evaluated volumes.
type Reaper struct {
	client       *proxmox.Client
	cfg          Config
	log          *slog.Logger
	observations map[string]time.Time
}

// New validates and constructs a Reaper.
func New(client *proxmox.Client, cfg Config, log *slog.Logger) (*Reaper, error) {
	if client == nil {
		return nil, errors.New("disk reaper: nil proxmox client")
	}
	if cfg.Storage == "" || len(cfg.Ranges) == 0 || cfg.Interval <= 0 || cfg.MinimumAge <= 0 {
		return nil, errors.New("disk reaper: storage, ranges, interval, and minimum age are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reaper{client: client, cfg: cfg, log: log}, nil
}

// Run performs an immediate sweep and then one sweep per interval. Individual
// sweep errors are logged and retried; a partial inventory must not take down
// the runner control plane or trigger deletion.
func (r *Reaper) Run(ctx context.Context) error {
	r.sweepAndLog(ctx)
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.sweepAndLog(ctx)
		}
	}
}

func (r *Reaper) sweepAndLog(ctx context.Context) {
	result, err := r.Sweep(ctx)
	if err != nil {
		r.log.Warn("disk reaper: sweep failed closed", "err", err)
		return
	}
	r.log.Info("disk reaper: sweep complete", "dry_run", r.cfg.DryRun,
		"candidates", len(result.Decisions), "eligible", len(result.Eligible))
}

// Sweep performs one complete inventory/evaluate/delete cycle.
func (r *Reaper) Sweep(ctx context.Context) (Result, error) {
	now := r.cfg.Now()
	guestConfigs, replications, candidates, observations, err := r.inventory(ctx, now)
	if err != nil {
		return Result{}, err
	}
	if err := saveObservations(r.cfg.StateFile, observations); err != nil {
		return Result{}, fmt.Errorf("persist observations: %w", err)
	}

	inv := Inventory{
		Ranges: r.cfg.Ranges, GuestConfigs: guestConfigs, ReplicationJobs: replications,
		Now: now, MinimumAge: r.cfg.MinimumAge,
	}
	result := Result{Decisions: make([]Decision, 0, len(candidates))}
	for _, candidate := range candidates {
		decision := Evaluate(candidate, inv)
		result.Decisions = append(result.Decisions, decision)
		if !decision.Eligible {
			continue
		}
		result.Eligible = append(result.Eligible, decision)
		r.log.Warn("disk reaper: eligible orphan disk", "dry_run", r.cfg.DryRun,
			"vmid", candidate.VMID, "dataset", candidate.Dataset, "node", candidate.Node,
			"age", decision.Age, "in_configured_range", decision.InConfiguredRange,
			"guest_config_absent", decision.GuestConfigAbsent,
			"replication_job_absent", decision.ReplicationJobAbsent, "old_enough", decision.OldEnough)
		if r.cfg.DryRun {
			continue
		}
		if err := r.revalidateReferences(ctx, candidate.VMID); err != nil {
			return result, fmt.Errorf("revalidate vmid %d before delete: %w", candidate.VMID, err)
		}
		node, nodeErr := r.client.Node(ctx, candidate.Node)
		if nodeErr != nil {
			return result, fmt.Errorf("re-resolve node %s before delete: %w", candidate.Node, nodeErr)
		}
		storage, storageErr := node.Storage(ctx, r.cfg.Storage)
		if storageErr != nil {
			return result, fmt.Errorf("re-resolve storage %s on %s before delete: %w", r.cfg.Storage, candidate.Node, storageErr)
		}
		task, deleteErr := storage.DeleteContent(ctx, candidate.Dataset)
		if deleteErr != nil {
			return result, fmt.Errorf("delete %s: %w", candidate.Dataset, deleteErr)
		}
		if waitErr := task.WaitFor(ctx, 120); waitErr != nil {
			return result, fmt.Errorf("await delete %s: %w", candidate.Dataset, waitErr)
		}
		r.log.Warn("disk reaper: reaped orphan disk", "vmid", candidate.VMID,
			"dataset", candidate.Dataset, "node", candidate.Node, "age", decision.Age,
			"in_configured_range", true, "guest_config_absent", true,
			"replication_job_absent", true, "old_enough", true)
	}
	return result, nil
}

// revalidateReferences closes the inventory-to-delete race. A clone can claim
// a previously config-less VMID between the sweep snapshot and DeleteContent;
// both cluster-wide guest config and replication absence must still hold at
// the destructive boundary.
func (r *Reaper) revalidateReferences(ctx context.Context, vmid int) error {
	cluster, err := r.client.Cluster(ctx)
	if err != nil {
		return err
	}
	resources, err := cluster.Resources(ctx, "vm")
	if err != nil {
		return err
	}
	for _, resource := range resources {
		resourceVMID, ok := checkedVMID(resource.VMID)
		if ok && resourceVMID == vmid {
			return fmt.Errorf("guest config appeared on node %s", resource.Node)
		}
	}
	jobs, err := cluster.ReplicationJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.Guest == vmid || strings.HasPrefix(job.ID, fmt.Sprintf("%d-", vmid)) {
			return fmt.Errorf("replication job %s appeared", job.ID)
		}
	}
	return nil
}

func (r *Reaper) inventory(ctx context.Context, now time.Time) (map[int]struct{}, map[int]struct{}, []Candidate, map[string]time.Time, error) {
	cluster, err := r.client.Cluster(ctx)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("cluster: %w", err)
	}
	resources, err := cluster.Resources(ctx, "vm")
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("list all guest configs: %w", err)
	}
	guests := make(map[int]struct{}, len(resources))
	for _, resource := range resources {
		if vmid, ok := checkedVMID(resource.VMID); ok && vmid != 0 {
			guests[vmid] = struct{}{}
		}
	}
	jobs, err := cluster.ReplicationJobs(ctx)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("list replication jobs: %w", err)
	}
	replications := make(map[int]struct{}, len(jobs))
	for _, job := range jobs {
		if job.Guest != 0 {
			replications[job.Guest] = struct{}{}
			continue
		}
		if prefix, _, ok := strings.Cut(job.ID, "-"); ok {
			var vmid int
			if _, scanErr := fmt.Sscanf(prefix, "%d", &vmid); scanErr == nil {
				replications[vmid] = struct{}{}
			}
		}
	}

	if r.observations == nil {
		r.observations, err = loadObservations(r.cfg.StateFile)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	observations := r.observations
	nodes, err := r.client.Nodes(ctx)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("list nodes: %w", err)
	}
	var candidates []Candidate
	seen := make(map[string]struct{})
	for _, nodeStatus := range nodes {
		node, nodeErr := r.client.Node(ctx, nodeStatus.Node)
		if nodeErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("node %s: %w", nodeStatus.Node, nodeErr)
		}
		storage, storageErr := node.Storage(ctx, r.cfg.Storage)
		if storageErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("storage %s on %s: %w", r.cfg.Storage, nodeStatus.Node, storageErr)
		}
		if storage.Enabled == 0 || storage.Active == 0 {
			continue
		}
		contents, contentErr := storage.GetContent(ctx)
		if contentErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("content %s on %s: %w", r.cfg.Storage, nodeStatus.Node, contentErr)
		}
		r.log.Debug("disk reaper: storage inventory", "node", nodeStatus.Node,
			"storage", r.cfg.Storage, "volumes", len(contents))
		for _, content := range contents {
			match := diskVolumeRE.FindStringSubmatch(content.Volid)
			if match == nil {
				continue
			}
			var vmid int
			if _, scanErr := fmt.Sscanf(match[1], "%d", &vmid); scanErr != nil {
				continue
			}
			key := nodeStatus.Node + "\x00" + content.Volid
			seen[key] = struct{}{}
			var createdAt time.Time
			if uint64(content.Ctime) <= math.MaxInt64 && content.Ctime != 0 {
				createdAt = time.Unix(int64(content.Ctime), 0) // #nosec G115 -- range checked above
			} else {
				createdAt = observations[key]
				if createdAt.IsZero() {
					createdAt = now
					observations[key] = now
				}
			}
			candidates = append(candidates, Candidate{VMID: vmid, Node: nodeStatus.Node,
				Dataset: content.Volid, Size: content.Used, CreatedAt: createdAt})
		}
	}
	for key := range observations {
		if _, ok := seen[key]; !ok {
			delete(observations, key)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].VMID != candidates[j].VMID {
			return candidates[i].VMID < candidates[j].VMID
		}
		return candidates[i].Dataset < candidates[j].Dataset
	})
	return guests, replications, candidates, observations, nil
}

func checkedVMID(value uint64) (int, bool) {
	if value > uint64(math.MaxInt) {
		return 0, false
	}
	return int(value), true // #nosec G115 -- range checked above
}

func loadObservations(path string) (map[string]time.Time, error) {
	out := make(map[string]time.Time)
	if path == "" {
		return out, nil
	}
	// #nosec G304 -- path is an operator-configured local state file, not request input.
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read observation state: %w", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode observation state: %w", err)
	}
	return out, nil
}

func saveObservations(path string, observations map[string]time.Time) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	raw, err := json.Marshal(observations)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".disk-reaper-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
