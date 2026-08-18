// Package provisioner is the Proxmox-facing side of the orchestrator. It
// turns a high-level intent ("clone a VM for the warm pool", "inject a JIT
// config into VM 10042", "destroy this VM") into the corresponding Proxmox
// VE API calls.
//
// All Provisioner methods accept a context.Context and propagate it to
// every underlying call. Network errors are wrapped with %w so callers can
// errors.Is them against package-level sentinel errors where needed.
package provisioner

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/jellydator/ttlcache/v3"
	"github.com/luthermonson/go-proxmox"

	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/config"
	"github.com/jeffresc/actions-runner-scaleset-proxmox/internal/tags"
)

// ErrTemplateNotFound is returned by [New] when the configured template
// VMID cannot be located on any node in the cluster.
var ErrTemplateNotFound = errors.New("provisioner: template VMID not found on any node")

// ErrLinkedCloneCrossNode indicates a Clone with Linked=true that targets a
// node different from the template's node. Linked clones must stay on the
// template's node because they reference its disks.
var ErrLinkedCloneCrossNode = errors.New("provisioner: linked clones must target the template's node")

// Operational sentinels callers can errors.Is against. These wrap the
// underlying go-proxmox / HTTP-status error so the detection live
// inside this package — callers never have to string-match Proxmox
// response text.
var (
	// ErrVMNotFound means the Proxmox API responded with "vm not
	// found" (typically HTTP 500 wrapping "Configuration file
	// 'nodes/.../qemu-server/<vmid>.conf' does not exist" or HTTP 404).
	// Destroy/Stop treat this as idempotent success.
	ErrVMNotFound = errors.New("provisioner: vm not found")

	// ErrVMAccessDenied means go-proxmox returned its typed
	// ErrNotAuthorized sentinel while resolving a VM. Destroy may treat
	// this as terminal only after a separate ownership-pool read proves
	// the VMID is not a current member; every other caller must surface it.
	ErrVMAccessDenied = errors.New("provisioner: vm access denied")

	// ErrVMAlreadyRunning is returned by Start when the VM is already
	// powered on. The caller's desired post-condition is already met.
	ErrVMAlreadyRunning = errors.New("provisioner: vm already running")

	// ErrGuestAgentNotReady is the transient class returned during the
	// brief window where the VM is up but the qemu-guest-agent socket
	// isn't responsive yet (firstboot scripts, systemd churn, etc.).
	// Callers (e.g. scaler.injectWithRetry) should retry with backoff
	// rather than burning the VM.
	ErrGuestAgentNotReady = errors.New("provisioner: qemu-guest-agent not ready")

	// ErrOwnershipMismatch means the current VM at a requested VMID is not a
	// member of this scale set's resource pool. Callers must abandon stale state
	// instead of retrying a destructive operation against that VMID.
	ErrOwnershipMismatch = errors.New("provisioner: live VM ownership mismatch")
)

// OwnershipMismatchError records the live Proxmox identity that caused a
// destructive operation to be refused.
type OwnershipMismatchError struct {
	VMID         int
	Node         string
	Name         string
	Tags         string
	Pool         string
	ExpectedPool string
}

func (e *OwnershipMismatchError) Error() string {
	return fmt.Sprintf("%v: vmid=%d node=%s name=%q pool=%q expected_pool=%q tags=%q",
		ErrOwnershipMismatch, e.VMID, e.Node, e.Name, e.Pool, e.ExpectedPool, e.Tags)
}

func (e *OwnershipMismatchError) Unwrap() error { return ErrOwnershipMismatch }

// VM is the orchestrator's view of a Proxmox VM. It is intentionally tiny —
// the persistent store (ent) carries the richer state.
//
// Profile is populated by ListOwnedVMs from the VM's profile tag (or
// the empty string when no profile tag is present, which Adopt then
// treats as the default profile). Other call sites that construct VM
// without a profile context can leave it empty.
type VM struct {
	VMID    int
	Node    string
	Name    string
	Profile string
}

// DestroyOutcome identifies an idempotent terminal result classified by its
// reason. The empty value means a VM was deleted normally.
// Keep this enum closed: it is exported as the bounded `reason` label on
// scaleset_destroy_terminal_total by the pool manager.
type DestroyOutcome string

const (
	// DestroyOutcomeNotFound means the VM was already absent.
	DestroyOutcomeNotFound DestroyOutcome = "not_found"
	// DestroyOutcomeAccessDenied means lookup was denied and the VMID was
	// independently proven absent from the ownership pool.
	DestroyOutcomeAccessDenied DestroyOutcome = "access_denied"
)

// DestroyOutcomeProvisioner is the outcome-aware destroy surface used by the
// pool manager. Provisioner.Destroy remains the compatibility surface for
// callers that only need idempotent success/failure semantics.
type DestroyOutcomeProvisioner interface {
	DestroyWithOutcome(ctx context.Context, vm *VM) (DestroyOutcome, error)
}

// CloneOptions are passed to [Provisioner.Clone]. NewVMID is allocated by
// the caller (pool manager); Node is chosen by the NodeSelector.
//
// Profile names the runner profile this clone belongs to (see
// internal/config.ProfileConfig). An empty Profile is treated as
// tags.DefaultProfile so callers that pre-date the profiles abstraction
// continue to work.
//
// TemplateVMID, CPUCores, MemoryMB, DiskGB, and Storage are optional
// per-clone overrides. Zero / empty inherits from the global Proxmox
// config (TemplateVMID) or from the template VM (CPU / memory / disk).
// CPU and memory are applied post-clone via VirtualMachine.Config; disk
// is resized via the resize endpoint when the requested size exceeds
// the template's current disk.
type CloneOptions struct {
	NewVMID   int
	Node      string
	Name      string
	Linked    bool
	PoweredOn bool

	Profile      string
	TemplateVMID int
	CPUCores     int
	MemoryMB     int
	DiskGB       int
	Storage      string
	Pool         string

	// TemplateClass is "stable" or "candidate" — stamped onto
	// the VM's Proxmox tag for canary attribution. Empty
	// defaults to "stable" via tags.Initial so callers without
	// a canary controller pay nothing.
	TemplateClass string

	// NICs, when non-empty, sets the cloned VM's network
	// interfaces post-clone (net0 is the first entry, net1 the
	// second, ...). Nil leaves the template's NICs in place. The
	// pool manager builds this from the profile's network config.
	NICs []CloneNIC

	// IPConfig, when non-empty, sets the Proxmox cloud-init
	// ipconfig0 field (e.g. "ip=10.0.0.10/24,gw=10.0.0.1"). The
	// VM boots with that address baked in by Proxmox's built-in
	// cloud-init drive. Empty leaves ipconfig0 untouched (DHCP
	// fallback or whatever the template configured).
	IPConfig string
}

// CloneNIC describes one network interface attachment on a
// cloned VM. Bridge is required; the rest are optional.
type CloneNIC struct {
	// Bridge is the Proxmox bridge name (e.g. "vmbr0"). Required.
	Bridge string

	// VLANTag tags the bridge; 0 = untagged (when VLANUntagged is
	// false, 0 means "skip the tag= attribute entirely" so the
	// bridge's VLAN-aware default applies).
	VLANTag int

	// VLANUntagged forces the NIC untagged even when VLANTag is
	// 0 — disambiguates the zero-vs-omitted case.
	VLANUntagged bool

	// MTU sets the link MTU. 0 inherits the bridge default.
	MTU int

	// Model is the virtio model string; empty defaults to
	// virtio (Proxmox's recommended high-performance driver).
	Model string
}

// Provisioner is the contract the rest of the orchestrator uses to talk to
// Proxmox. The proxmox-backed implementation (see [New]) is the only
// implementation in production.
type Provisioner interface {
	Clone(ctx context.Context, opts CloneOptions) (*VM, error)
	Start(ctx context.Context, vm *VM) error
	Stop(ctx context.Context, vm *VM) error
	Destroy(ctx context.Context, vm *VM) error
	WaitReady(ctx context.Context, vm *VM, timeout time.Duration) error
	InjectJITConfig(ctx context.Context, vm *VM, jitConfig string) error
	ReadJITConfig(ctx context.Context, vm *VM) ([]byte, error)
	ListOwnedVMs(ctx context.Context) ([]*VM, error)

	// PowerState returns the Proxmox status string for the VM —
	// typically "running", "stopped", or "paused". Returns an empty
	// string when the VM cannot be located (callers should treat that
	// as "unknown" and skip — not as "stopped"). Used by the pool
	// manager's power-state poller to detect job completion: the
	// in-VM gh-runner.service powers off after the runner exits, and
	// observing "stopped" on an Assigned/Running row is the orchestrator's
	// completion signal in lieu of an in-VM hook.
	PowerState(ctx context.Context, vm *VM) (string, error)

	// Ping does the cheapest possible Proxmox API call (GET /version) so
	// callers can drive readiness probes. Returns nil iff the API is
	// reachable + the configured credentials still work.
	Ping(ctx context.Context) error

	// TemplateNode reports the node the template VM lives on. Useful for
	// the pool manager when deciding where to place a linked clone.
	TemplateNode() string

	// Client returns the underlying Proxmox client. Exposed for code
	// that needs to issue calls outside the Provisioner's typed surface
	// (e.g. the least-loaded NodeSelector). Callers must not retain the
	// pointer past the Provisioner's lifetime.
	Client() *proxmox.Client

	// IsRecentlyDestroyed reports whether the VMID's qmdestroy task
	// completed within the given cooldown window. The pool's VMID
	// allocator consults this so a freshly freed VMID isn't reissued
	// while PVE-side lock-file cleanup is still settling — which
	// otherwise produces "VM N is running - destroy failed" errors
	// and 60s lock-file timeouts.
	IsRecentlyDestroyed(vmid int, cooldown time.Duration) bool
	QuarantineVMID(vmid int)
	IsVMIDQuarantined(vmid int) bool

	// InFlightCloneCount returns the number of clones currently inside
	// Clone() between the PVE qmclone task returning and the follow-up
	// qmconfig that applies our owner tags. The pool's headroom
	// calculation adds this to stats.Provisioning so reconcile ticks
	// can't double-dispatch clones that the previous tick has in flight.
	InFlightCloneCount() int
}

// OwnershipResidualAuditor is implemented by production provisioners that can
// detect runner-shaped VMs outside their configured ownership pool.
type OwnershipResidualAuditor interface {
	CountUnpooledRunnerVMs(context.Context, []*VM) (int, error)
}

// pmox is the production Provisioner backed by github.com/luthermonson/go-proxmox.
type pmox struct {
	cfg          config.ProxmoxConfig
	cli          *proxmox.Client
	scaleSetName string
	poolID       string
	templateNode string
	log          *slog.Logger

	// inFlightClones tracks VMIDs currently inside Clone() between the
	// PVE qmclone task returning and the follow-up qmconfig that applies
	// our owner tags. Values are the timestamp the entry was inserted
	// (kept for diagnostics; the library handles expiry).
	//
	// The cache's TTL bounds how long an entry survives in case Clone
	// hangs and never returns to clear it. Set via the constructor from
	// pool.clone_inflight_grace; zero falls back to a 5m default.
	inFlightClones *ttlcache.Cache[int, time.Time]

	// recentlyDestroyed tracks VMIDs whose Proxmox qmdestroy task has
	// recently completed. The pool's allocateVMID consults this via
	// IsRecentlyDestroyed to avoid reissuing a VMID while PVE-side
	// lock-file cleanup is still settling — which would otherwise
	// produce "VM N is running - destroy failed" errors.
	//
	// The cache's TTL bounds entry lifetime; it is a memory ceiling, not
	// the cooldown — callers of IsRecentlyDestroyed pass their own
	// (typically shorter) cooldown. Set via the constructor from
	// pool.vmid_reuse_cooldown × 4; zero falls back to a 10m default.
	recentlyDestroyed *ttlcache.Cache[int, time.Time]

	// quarantinedVMIDs contains live ownership mismatches and expired store
	// rows. It intentionally lasts only for this provisioner process.
	quarantinedVMIDs sync.Map
}

// Options configures Provisioner trackers separate from the static
// Proxmox connection settings. Zero values fall back to safe defaults.
type Options struct {
	// CloneInflightTTL bounds how long an in-flight clone entry may
	// live before the background sweep prunes it. Protects against
	// a Clone() that hangs and never reaches its defer-Delete.
	// Defaults to 5 minutes.
	CloneInflightTTL time.Duration

	// RecentlyDestroyedTTL bounds how long a destroyed-VMID entry
	// stays in the cooldown map. The pool's allocateVMID consults
	// IsRecentlyDestroyed with its own (shorter) cooldown, so this
	// TTL is purely a memory ceiling for the map — pick something
	// comfortably above the longest plausible vmid_reuse_cooldown.
	// Defaults to 10 minutes.
	RecentlyDestroyedTTL time.Duration
}

// New constructs a Proxmox-backed Provisioner. It performs a one-time
// scan of the cluster to locate the template VMID and caches the result.
//
// The provided ctx governs the ttlcache background eviction goroutines
// that prune stale in-flight clone and recently-destroyed entries.
// Cancel the context to stop the trackers at shutdown.
func New(ctx context.Context, cfg config.ProxmoxConfig, scaleSetName, poolID string, opts Options, log *slog.Logger) (Provisioner, error) {
	if log == nil {
		log = slog.Default()
	}
	if opts.CloneInflightTTL <= 0 {
		opts.CloneInflightTTL = 5 * time.Minute
	}
	if opts.RecentlyDestroyedTTL <= 0 {
		opts.RecentlyDestroyedTTL = 10 * time.Minute
	}
	cli := newProxmoxClient(cfg)
	p := &pmox{
		cfg:               cfg,
		cli:               cli,
		scaleSetName:      scaleSetName,
		poolID:            poolID,
		log:               log,
		inFlightClones:    newTracker(opts.CloneInflightTTL),
		recentlyDestroyed: newTracker(opts.RecentlyDestroyedTTL),
	}
	if err := p.discoverTemplateNode(ctx); err != nil {
		return nil, err
	}
	// ttlcache.Start runs a background eviction loop; stop it when ctx
	// fires. Without WithDisableTouchOnHit a read would extend the TTL,
	// which would mask a hung Clone() exactly when the entry is supposed
	// to be pruned — and would lengthen recentlyDestroyed cooldown beyond
	// the configured ceiling.
	go p.inFlightClones.Start()
	go p.recentlyDestroyed.Start()
	go func() {
		<-ctx.Done()
		p.inFlightClones.Stop()
		p.recentlyDestroyed.Stop()
	}()
	log.Info("provisioner ready", "template_vmid", cfg.TemplateVMID, "template_node", p.templateNode)
	return p, nil
}

// newTracker constructs a VMID→insertion-timestamp cache with the given
// TTL. WithDisableTouchOnHit keeps reads from extending the TTL — the
// TTL is meant to bound how long a leaked entry survives, not to be
// reset every time the orchestrator looks at it.
func newTracker(ttl time.Duration) *ttlcache.Cache[int, time.Time] {
	return ttlcache.New[int, time.Time](
		ttlcache.WithTTL[int, time.Time](ttl),
		ttlcache.WithDisableTouchOnHit[int, time.Time](),
	)
}

// newProxmoxClient builds the underlying HTTP+API-token client.
//
// The HTTP transport is wrapped by hashicorp/go-retryablehttp so transient
// Proxmox API hiccups (502/503/504 during pveproxy restarts, DNS blips,
// short-lived connection errors) are retried with exponential backoff
// before surfacing to the caller. Idempotent operations (GET / inspect)
// retry up to 4 times; the underlying library only retries on its
// CheckRetry predicate, which treats 5xx + connection errors as retryable
// but NOT 4xx (e.g. 403/404 fail-fast).
func newProxmoxClient(cfg config.ProxmoxConfig) *proxmox.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// Pin TLS 1.2 floor (RFC 8996 deprecates TLS 1.0/1.1) regardless of
	// the InsecureSkipVerify opt-in — skipping cert verification doesn't
	// mean we should also accept deprecated protocol versions.
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		tr.TLSClientConfig.MinVersion = tls.VersionTLS12
	}
	if cfg.InsecureSkipVerify {
		tr.TLSClientConfig.InsecureSkipVerify = true //nolint:gosec // user-opt-in
	}

	retry := retryablehttp.NewClient()
	retry.HTTPClient = &http.Client{
		Transport: tr,
		// Per-request hard cap. Must be >= the largest task.WaitFor
		// budget (600s in Clone) — otherwise a single poll that stalls
		// past Timeout aborts a still-progressing task and the caller
		// sees a spurious failure. 15m matches the per-clone ctx
		// deadline in pool.runClone so this is just a backstop for
		// callers that forget to pass a bounded ctx; well-behaved
		// callers' ctx deadlines win long before this fires.
		Timeout: 15 * time.Minute,
	}
	retry.RetryMax = 4
	retry.RetryWaitMin = 500 * time.Millisecond
	retry.RetryWaitMax = 8 * time.Second
	// Silence the retryable-http default logger; orchestrator logs the
	// final outcome via the provisioner's slog.
	retry.Logger = nil
	retry.ErrorHandler = func(resp *http.Response, _ error, _ int) (*http.Response, error) {
		// Returning the last response (not an error) lets the typed
		// Proxmox client handle the HTTP status normally, including its
		// own 4xx/5xx error decoding.
		return resp, nil
	}

	hc := retry.StandardClient()
	return proxmox.NewClient(cfg.Endpoint,
		proxmox.WithHTTPClient(hc),
		proxmox.WithAPIToken(cfg.Auth.TokenID, cfg.Auth.TokenSecret),
	)
}

func (p *pmox) TemplateNode() string    { return p.templateNode }
func (p *pmox) Client() *proxmox.Client { return p.cli }

// IsRecentlyDestroyed returns true iff a qmdestroy for vmid completed
// within the cooldown window. The caller-supplied cooldown is checked
// against the entry's insertion timestamp; this is distinct from the
// cache TTL, which is a longer memory-ceiling. Entries are
// checked-and-evicted on access — an entry past its cooldown is
// removed the next time the same VMID is looked up — so the cache is
// lazily cleared but always returns ground truth on read.
func (p *pmox) IsRecentlyDestroyed(vmid int, cooldown time.Duration) bool {
	item := p.recentlyDestroyed.Get(vmid)
	if item == nil {
		return false
	}
	if time.Since(item.Value()) >= cooldown {
		p.recentlyDestroyed.Delete(vmid)
		return false
	}
	return true
}

func (p *pmox) QuarantineVMID(vmid int) {
	p.quarantinedVMIDs.Store(vmid, struct{}{})
}

func (p *pmox) IsVMIDQuarantined(vmid int) bool {
	_, ok := p.quarantinedVMIDs.Load(vmid)
	return ok
}

// InFlightCloneCount returns the number of Clone() calls currently in
// flight. Used by the pool's headroom calculation; see the interface
// doc for the rationale.
func (p *pmox) InFlightCloneCount() int {
	return p.inFlightClones.Len()
}

// Ping issues a GET /version against the Proxmox API. This is the
// canonical cheapest call and is the basis for readiness signalling.
func (p *pmox) Ping(ctx context.Context) error {
	if _, err := p.cli.Version(ctx); err != nil {
		return fmt.Errorf("proxmox ping: %w", err)
	}
	return nil
}

// templateDiscoveryTimeoutPerNode caps how long a single per-node call
// in discoverTemplateNode may block. Without it, one unreachable
// Proxmox node would pin startup forever — the scan is sequential and
// the underlying HTTP client has no per-request deadline once a
// connection is established. Tests may override this.
var templateDiscoveryTimeoutPerNode = 30 * time.Second

// discoverTemplateNode walks the cluster to find the node hosting the
// configured template VMID. If a node has the VMID but the VM isn't a
// template, the scan continues (the VMID might appear on multiple nodes
// in some HA configurations) and the non-template hit is reported at
// end if no real template was found.
//
// Each per-node interaction is bounded by templateDiscoveryTimeoutPerNode
// so a single hung node can't pin startup.
func (p *pmox) discoverTemplateNode(ctx context.Context) error {
	statuses, err := p.cli.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("provisioner: list nodes: %w", err)
	}
	var nonTemplateHits []string
	for _, ns := range statuses {
		nodeCtx, cancel := context.WithTimeout(ctx, templateDiscoveryTimeoutPerNode)
		node, err := p.cli.Node(nodeCtx, ns.Node)
		if err != nil {
			cancel()
			p.log.Warn("provisioner: get node failed; continuing scan", "node", ns.Node, "err", err)
			continue
		}
		vm, err := node.VirtualMachine(nodeCtx, p.cfg.TemplateVMID)
		cancel()
		if err != nil {
			continue
		}
		if !isTemplate(vm) {
			p.log.Warn("provisioner: VMID found but not a template; continuing scan",
				"vmid", p.cfg.TemplateVMID, "node", ns.Node)
			nonTemplateHits = append(nonTemplateHits, ns.Node)
			continue
		}
		p.templateNode = ns.Node
		return nil
	}
	if len(nonTemplateHits) > 0 {
		return fmt.Errorf("%w: vmid=%d found on %v but none are templates",
			ErrTemplateNotFound, p.cfg.TemplateVMID, nonTemplateHits)
	}
	return fmt.Errorf("%w: vmid=%d", ErrTemplateNotFound, p.cfg.TemplateVMID)
}

func isTemplate(vm *proxmox.VirtualMachine) bool {
	return bool(vm.Template)
}

// Clone clones the template VM into NewVMID on opts.Node, applies our owner
// tags, and optionally starts it.
//
// NewVMID enters the in-flight tracker only after qmclone completes
// successfully. It remains there until owner tags land or cleanup destroys
// that known-created VM. A clone collision therefore never gains an ownership
// exemption; the TTL remains a safety net for interrupted cleanup.
func (p *pmox) Clone(ctx context.Context, opts CloneOptions) (*VM, error) {
	if opts.Linked && opts.Node != p.templateNode {
		return nil, fmt.Errorf("%w: requested node=%s template_node=%s", ErrLinkedCloneCrossNode, opts.Node, p.templateNode)
	}
	templateVMID := resolveTemplateVMID(opts.TemplateVMID, p.cfg.TemplateVMID)
	templateNodeName, err := p.resolveTemplateNode(ctx, opts)
	if err != nil {
		return nil, err
	}

	templateNode, err := p.cli.Node(ctx, templateNodeName)
	if err != nil {
		return nil, fmt.Errorf("get template node: %w", err)
	}
	templateVM, err := templateNode.VirtualMachine(ctx, templateVMID)
	if err != nil {
		return nil, fmt.Errorf("get template vm: %w", err)
	}

	newID, task, err := templateVM.Clone(ctx, buildLibCloneOptions(opts, templateNodeName))
	if err != nil {
		return nil, fmt.Errorf("issue clone: %w", err)
	}
	if err := awaitTask(ctx, task, 600); err != nil {
		return nil, fmt.Errorf("await clone task: %w", err)
	}
	p.inFlightClones.Set(opts.NewVMID, time.Now(), ttlcache.DefaultTTL)

	// Compute the resulting node; linked clones land on the template node.
	resultNode := opts.Node
	if opts.Linked {
		resultNode = templateNodeName
	}
	if resultNode == "" {
		resultNode = templateNodeName
	}

	newNode, err := p.cli.Node(ctx, resultNode)
	if err != nil {
		return nil, fmt.Errorf("get new node: %w", err)
	}
	newVM, err := newNode.VirtualMachine(ctx, newID)
	if err != nil {
		return nil, fmt.Errorf("fetch cloned vm: %w", err)
	}

	configOpts, err := buildCloneConfig(p.scaleSetName, opts)
	if err != nil {
		return nil, err
	}
	task, err = newVM.Config(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("set owner tags / overrides: %w", err)
	}
	if err := awaitTask(ctx, task, 60); err != nil {
		return nil, fmt.Errorf("await owner tag / override task: %w", err)
	}
	// Owner metadata and hardware overrides are confirmed applied; clone-
	// failure cleanup no longer needs the in-flight marker.
	p.inFlightClones.Delete(opts.NewVMID)

	// Disk resize is a distinct API endpoint (not Config). Apply after
	// the Config call so the override is visible to the resize call.
	// Proxmox treats an unprefixed value as absolute; a leading '+'
	// would mean "grow by N". We pass absolute so the value lines up
	// with the operator's stated profile.disk_gb regardless of the
	// template's current disk size.
	if opts.DiskGB > 0 {
		task, err := newVM.ResizeDisk(ctx, "scsi0", fmt.Sprintf("%dG", opts.DiskGB))
		if err != nil {
			return nil, fmt.Errorf("resize disk: %w", err)
		}
		if err := awaitTask(ctx, task, 120); err != nil {
			return nil, fmt.Errorf("await resize disk: %w", err)
		}
	}

	if opts.PoweredOn {
		if err := p.startInternal(ctx, newVM); err != nil {
			return nil, err
		}
	}

	return &VM{VMID: newID, Node: resultNode, Name: opts.Name}, nil
}

// resolveTemplateVMID picks the source template VMID for a clone:
// the per-clone override (opts.TemplateVMID) wins when positive,
// otherwise we fall back to the orchestrator-global template.
// Pure function so the canary / profile-override decision is
// straightforward to test in isolation.
func resolveTemplateVMID(optOverride, defaultTemplateVMID int) int {
	if optOverride > 0 {
		return optOverride
	}
	return defaultTemplateVMID
}

// resolveTemplateNode picks the node where the source template
// lives. Linked clones must stay on the orchestrator's template
// node by construction; profile-override templates typically live
// on the same node but we re-discover when the override is
// explicitly set and differs from the global template, since the
// operator may have placed it elsewhere.
func (p *pmox) resolveTemplateNode(ctx context.Context, opts CloneOptions) (string, error) {
	if opts.TemplateVMID > 0 && opts.TemplateVMID != p.cfg.TemplateVMID {
		discovered, err := p.locateTemplate(ctx, opts.TemplateVMID)
		if err != nil {
			return "", fmt.Errorf("locate profile template %d: %w", opts.TemplateVMID, err)
		}
		return discovered, nil
	}
	return p.templateNode, nil
}

// buildLibCloneOptions translates our CloneOptions into the
// go-proxmox library shape. Linked clones set Full=0 and leave
// Target/Storage unset (the library mandates the new VM stays on
// the template node). Full clones may optionally Target a different
// node and Storage a different pool.
func buildLibCloneOptions(opts CloneOptions, templateNodeName string) *proxmox.VirtualMachineCloneOptions {
	cloneOpts := &proxmox.VirtualMachineCloneOptions{
		NewID: opts.NewVMID,
		Name:  opts.Name,
		Pool:  opts.Pool,
	}
	if opts.Linked {
		cloneOpts.Full = proxmox.IntOrBool(false)
		return cloneOpts
	}
	cloneOpts.Full = proxmox.IntOrBool(true)
	if opts.Node != "" && opts.Node != templateNodeName {
		cloneOpts.Target = opts.Node
	}
	if opts.Storage != "" {
		cloneOpts.Storage = opts.Storage
	}
	return cloneOpts
}

// buildCloneConfig assembles the per-clone VirtualMachineOption
// slice applied to the freshly-cloned VM. It bundles owner +
// profile tags with hardware overrides (cpu/memory/NICs/ipconfig0)
// so the orchestrator can apply everything in a single Config call
// — keeping the tag-apply atomic with the resource override.
//
// Disk resize is intentionally out of scope: it goes through a
// distinct Proxmox endpoint (ResizeDisk) and is applied separately
// after the Config call lands.
func buildCloneConfig(scaleSetName string, opts CloneOptions) ([]proxmox.VirtualMachineOption, error) {
	initial, err := tags.Initial(scaleSetName, opts.Profile, opts.TemplateClass)
	if err != nil {
		return nil, fmt.Errorf("compute initial tags: %w", err)
	}
	configOpts := []proxmox.VirtualMachineOption{
		{Name: "tags", Value: tags.Encode(initial)},
	}
	if opts.CPUCores > 0 {
		configOpts = append(configOpts, proxmox.VirtualMachineOption{Name: "cores", Value: opts.CPUCores})
	}
	if opts.MemoryMB > 0 {
		configOpts = append(configOpts, proxmox.VirtualMachineOption{Name: "memory", Value: opts.MemoryMB})
	}
	for i, nic := range opts.NICs {
		configOpts = append(configOpts, proxmox.VirtualMachineOption{
			Name:  fmt.Sprintf("net%d", i),
			Value: encodeNIC(nic),
		})
	}
	if opts.IPConfig != "" {
		configOpts = append(configOpts, proxmox.VirtualMachineOption{Name: "ipconfig0", Value: opts.IPConfig})
	}
	return configOpts, nil
}

// locateTemplate finds the node hosting an alternative template VMID
// (e.g. a profile-specific template that differs from the orchestrator's
// default). Uses the same per-node timeout strategy as
// discoverTemplateNode so a hung node can't pin the clone.
func (p *pmox) locateTemplate(ctx context.Context, templateVMID int) (string, error) {
	statuses, err := p.cli.Nodes(ctx)
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	for _, ns := range statuses {
		nodeCtx, cancel := context.WithTimeout(ctx, templateDiscoveryTimeoutPerNode)
		node, err := p.cli.Node(nodeCtx, ns.Node)
		if err != nil {
			cancel()
			continue
		}
		vm, err := node.VirtualMachine(nodeCtx, templateVMID)
		cancel()
		if err != nil {
			continue
		}
		if !isTemplate(vm) {
			continue
		}
		return ns.Node, nil
	}
	return "", fmt.Errorf("%w: vmid=%d", ErrTemplateNotFound, templateVMID)
}

// Start powers on an existing VM and waits up to 5 minutes for the task to
// settle. The VM may not yet have a working guest agent on return; call
// WaitReady to confirm.
func (p *pmox) Start(ctx context.Context, vm *VM) error {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		return err
	}
	return p.startInternal(ctx, pVM)
}

func (p *pmox) startInternal(ctx context.Context, pVM *proxmox.VirtualMachine) error {
	task, err := pVM.Start(ctx)
	if err != nil {
		// If the VM is already running, Proxmox returns an error. Treat as
		// success since the desired post-condition is met.
		if isAlreadyRunning(err) {
			return nil
		}
		return fmt.Errorf("start vm: %w", err)
	}
	if err := awaitTask(ctx, task, 300); err != nil {
		return fmt.Errorf("await start: %w", err)
	}
	return nil
}

// Stop attempts a graceful Shutdown first; if that doesn't settle within
// 60s it falls back to a hard Stop.
func (p *pmox) Stop(ctx context.Context, vm *VM) error {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		if isNotFound(err) {
			p.inFlightClones.Delete(vm.VMID)
			return nil
		}
		return err
	}
	if err := p.requireDestructiveOwnership(ctx, pVM); err != nil {
		return err
	}
	return p.stopInternal(ctx, pVM)
}

func (p *pmox) requireDestructiveOwnership(ctx context.Context, pVM *proxmox.VirtualMachine) error {
	vmid := int(pVM.VMID) // #nosec G115 -- Proxmox VMIDs are bounded integers.
	member, err := p.isOwnershipPoolMember(ctx, vmid)
	if err != nil {
		return err
	}
	if member {
		return nil
	}
	observedPool := ""
	if cluster, clusterErr := p.cli.Cluster(ctx); clusterErr == nil {
		if resources, resourcesErr := cluster.Resources(ctx, "vm"); resourcesErr == nil {
			for _, resource := range resources {
				resourceVMID := int(resource.VMID) // #nosec G115 -- Proxmox VMIDs are bounded integers.
				if resourceVMID == vmid {
					observedPool = resource.Pool
					break
				}
			}
		}
	}
	p.QuarantineVMID(vmid)
	return &OwnershipMismatchError{
		VMID: vmid, Node: pVM.Node, Name: pVM.Name, Tags: pVM.Tags,
		Pool: observedPool, ExpectedPool: p.poolID,
	}
}

// isOwnershipPoolMember is the single authoritative membership read used by
// both the ordinary destructive-ownership gate and the access-denied lookup
// path. Membership is ownership; VM names and tags are metadata only.
func (p *pmox) isOwnershipPoolMember(ctx context.Context, vmid int) (bool, error) {
	pool, err := p.cli.Pool(ctx, p.poolID, "qemu")
	if err != nil {
		return false, fmt.Errorf("read ownership pool %q: %w", p.poolID, err)
	}
	for _, member := range pool.Members {
		memberVMID := int(member.VMID) // #nosec G115 -- Proxmox VMIDs are bounded integers.
		if memberVMID == vmid {
			return true, nil
		}
	}
	return false, nil
}

// stopInternal does the graceful-then-hard stop with an already-resolved
// *proxmox.VirtualMachine. Lets callers (notably Destroy) reuse a handle
// they already fetched instead of paying the GET-node + GET-vm round
// trips a second time.
func (p *pmox) stopInternal(ctx context.Context, pVM *proxmox.VirtualMachine) error {
	// Best-effort graceful shutdown.
	task, err := pVM.Shutdown(ctx)
	if err == nil {
		werr := awaitTask(ctx, task, 60)
		if werr == nil {
			return nil
		}
		// On drain/SIGTERM the graceful WaitFor returns context.Canceled.
		// Don't mask that as a "graceful shutdown timed out → hard stop"
		// fallback: the hard Stop below would run against the same
		// already-cancelled ctx and surface a misleading error. Surface
		// the cancellation to the caller instead.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.log.Warn("graceful shutdown timed out; falling back to hard stop", "vmid", pVM.VMID, "node", pVM.Node)
	}
	task, err = pVM.Stop(ctx)
	if err != nil {
		return fmt.Errorf("hard stop: %w", classifyProxmoxError(err))
	}
	if err := awaitTask(ctx, task, 60); err != nil {
		return fmt.Errorf("await hard stop: %w", classifyProxmoxError(err))
	}
	return nil
}

// Destroy stops (if needed) and deletes a VM. Idempotent terminal outcomes are
// returned as success; use DestroyWithOutcome when the caller needs to count
// why no ordinary delete task ran.
func (p *pmox) Destroy(ctx context.Context, vm *VM) error {
	_, err := p.DestroyWithOutcome(ctx, vm)
	return err
}

// DestroyWithOutcome is Destroy plus a closed terminal-outcome result. A
// permission denial is terminal only when the independent ownership-pool read
// proves the VMID is outside the pool. A denial for a current member remains a
// hard error, preserving genuine ACL-regression visibility.
func (p *pmox) DestroyWithOutcome(ctx context.Context, vm *VM) (DestroyOutcome, error) {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		return p.destroyLookupOutcome(ctx, vm, err)
	}
	if err := p.requireDestructiveOwnership(ctx, pVM); err != nil {
		return "", err
	}
	// Reuse the resolved handle for the stop step — Destroy is on the
	// hot drain path so an extra round trip per VM matters at scale.
	if err := p.stopInternal(ctx, pVM); err != nil {
		p.log.Warn("stop before destroy failed; proceeding to delete anyway", "vmid", vm.VMID, "err", err)
	}
	// go-proxmox's VirtualMachine.Delete first removes the optional custom
	// cloud-init ISO created by VirtualMachine.CloudInit. Calling the raw
	// endpoint below would otherwise skip that cleanup. The scaleset does not
	// create those ISOs today, but preserving the library behaviour here keeps
	// a future CloudInit caller from trading the disk leak for an ISO leak.
	if err := p.deleteCloudInitISO(ctx, pVM); err != nil {
		return "", fmt.Errorf("delete cloud-init iso: %w", classifyProxmoxError(err))
	}

	params := url.Values{}
	params.Set("destroy-unreferenced-disks", "1")
	params.Set("purge", "1")
	path := fmt.Sprintf("/nodes/%s/qemu/%d?%s", pVM.Node, pVM.VMID, params.Encode())
	var upid proxmox.UPID
	err = p.cli.Delete(ctx, path, &upid)
	if err != nil {
		if isNotFound(err) {
			p.inFlightClones.Delete(vm.VMID)
			return DestroyOutcomeNotFound, nil
		}
		return "", fmt.Errorf("delete vm: %w", classifyProxmoxError(err))
	}
	task := proxmox.NewTask(upid, p.cli)
	if err := awaitTask(ctx, task, 120); err != nil {
		// Mid-task 404 → idempotent success (VM disappeared while we
		// were tearing it down — common with another orchestrator).
		classified := classifyProxmoxError(err)
		if errors.Is(classified, ErrVMNotFound) {
			p.inFlightClones.Delete(vm.VMID)
			p.recentlyDestroyed.Set(vm.VMID, time.Now(), ttlcache.DefaultTTL)
			return DestroyOutcomeNotFound, nil
		}
		return "", fmt.Errorf("await delete: %w", classified)
	}
	p.inFlightClones.Delete(vm.VMID)
	// PVE has finished the qmdestroy task. Record the timestamp so the
	// pool's allocateVMID skips this VMID until the configured cooldown
	// elapses — without this, a fresh clone targeting the same VMID
	// would race PVE-side lock-file cleanup and produce
	// "VM N is running - destroy failed" errors.
	p.recentlyDestroyed.Set(vm.VMID, time.Now(), ttlcache.DefaultTTL)
	return "", nil
}

// destroyLookupOutcome classifies a getVM failure and applies the pool
// membership safety guard required for permission denials. Kept separate from
// DestroyWithOutcome so the otherwise-unreproducible 403 branch can be tested
// deterministically without manufacturing a live ACL failure.
func (p *pmox) destroyLookupOutcome(ctx context.Context, vm *VM, err error) (DestroyOutcome, error) {
	classified := classifyProxmoxError(err)
	if errors.Is(classified, ErrVMNotFound) {
		p.inFlightClones.Delete(vm.VMID)
		return DestroyOutcomeNotFound, nil
	}
	if !errors.Is(classified, ErrVMAccessDenied) {
		return "", err
	}
	member, membershipErr := p.isOwnershipPoolMember(ctx, vm.VMID)
	if membershipErr != nil {
		return "", fmt.Errorf("verify ownership after access denial for vm %d: %w", vm.VMID, membershipErr)
	}
	if !member {
		p.inFlightClones.Delete(vm.VMID)
		return DestroyOutcomeAccessDenied, nil
	}
	return "", fmt.Errorf("destroy vm %d: access denied for current ownership-pool member: %w", vm.VMID, classified)
}

// deleteCloudInitISO mirrors go-proxmox VirtualMachine.Delete's private
// pre-delete helper. The library tags only its custom user-data ISO flow with
// "proxmox-cloud-init"; Proxmox's ordinary built-in cloud-init drive does not
// use this path.
func (p *pmox) deleteCloudInitISO(ctx context.Context, vm *proxmox.VirtualMachine) error {
	if !vm.HasTag(proxmox.MakeTag(proxmox.TagCloudInit)) {
		return nil
	}

	node, err := p.cli.Node(ctx, vm.Node)
	if err != nil {
		return err
	}
	storages, err := node.Storages(ctx)
	if err != nil {
		return err
	}

	isoFilename := fmt.Sprintf(proxmox.UserDataISOFormat, vm.VMID)
	for _, storage := range storages {
		if storage.Enabled == 0 || !strings.Contains(storage.Content, "iso") {
			continue
		}
		iso, isoErr := storage.ISO(ctx, isoFilename)
		if isoErr != nil {
			continue
		}
		task, deleteErr := iso.Delete(ctx)
		if deleteErr != nil {
			return deleteErr
		}
		return task.WaitFor(ctx, 5)
	}
	return nil
}

// PowerState returns the Proxmox status string ("running"/"stopped"/...)
// for the VM. A missing VM returns ("", nil) — callers treat that as
// "unknown" and skip the row rather than confuse it with "stopped".
func (p *pmox) PowerState(ctx context.Context, vm *VM) (string, error) {
	if vm == nil {
		return "", fmt.Errorf("power state: nil vm")
	}
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		if isNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return pVM.Status, nil
}

// WaitReady blocks until the qemu-guest-agent inside the VM is responsive
// or the timeout elapses. Errors are routed through classifyProxmoxError
// so callers can errors.Is against ErrVMNotFound / ErrGuestAgentNotReady
// regardless of which library internal raised the underlying failure.
func (p *pmox) WaitReady(ctx context.Context, vm *VM, timeout time.Duration) error {
	pVM, err := p.getVM(ctx, vm)
	if err != nil {
		return classifyProxmoxError(err)
	}
	seconds := int(timeout.Seconds())
	if seconds < 1 {
		seconds = 60
	}
	if err := pVM.WaitForAgent(ctx, seconds); err != nil {
		return fmt.Errorf("await guest agent: %w", classifyProxmoxError(err))
	}
	return nil
}

// ListOwnedVMs returns the members of this scaleset's Proxmox resource pool.
// Pool membership is the ownership proof; tags remain routing metadata.
func (p *pmox) ListOwnedVMs(ctx context.Context) ([]*VM, error) {
	pool, err := p.cli.Pool(ctx, p.poolID, "qemu")
	if err != nil {
		return nil, fmt.Errorf("list ownership pool %q: %w", p.poolID, err)
	}
	out := make([]*VM, 0, len(pool.Members))
	for _, member := range pool.Members {
		out = append(out, &VM{
			VMID: int(member.VMID), // #nosec G115 -- Proxmox VMIDs are bounded integers.
			Node: member.Node, Name: member.Name,
			Profile: tags.ProfileOf(member.Tags),
		})
	}
	return out, nil
}

// CountUnpooledRunnerVMs reports tracked runner VMs outside the ownership
// pool. It is detection only and does not grant destructive authority.
func (p *pmox) CountUnpooledRunnerVMs(ctx context.Context, candidates []*VM) (int, error) {
	pool, err := p.cli.Pool(ctx, p.poolID, "qemu")
	if err != nil {
		return 0, fmt.Errorf("list ownership pool %q: %w", p.poolID, err)
	}
	members := make(map[int]struct{}, len(pool.Members))
	for _, member := range pool.Members {
		memberVMID := int(member.VMID) // #nosec G115 -- Proxmox VMIDs are bounded integers.
		members[memberVMID] = struct{}{}
	}
	count := 0
	for _, candidate := range candidates {
		if _, ok := members[candidate.VMID]; !ok {
			count++
		}
	}
	return count, nil
}

// getVM resolves *VM into the library's *proxmox.VirtualMachine.
func (p *pmox) getVM(ctx context.Context, vm *VM) (*proxmox.VirtualMachine, error) {
	node, err := p.cli.Node(ctx, vm.Node)
	if err != nil {
		return nil, fmt.Errorf("get node %s: %w", vm.Node, err)
	}
	pVM, err := node.VirtualMachine(ctx, vm.VMID)
	if err != nil {
		return nil, fmt.Errorf("get vm %d on %s: %w", vm.VMID, vm.Node, err)
	}
	return pVM, nil
}

// errorClassifier inspects err and returns a wrapped form with our
// typed sentinel when it recognises the underlying go-proxmox / HTTP
// pattern. The bool reports recognition; on false the dispatcher moves
// on to the next classifier. Splitting the detection layers (typed
// errors → HTTP status → body text) into independent classifiers
// makes each one testable in isolation and keeps the priority order
// explicit at the table site below.
type errorClassifier func(err error) (ok bool, wrapped error)

// classifyTypedError catches the library-supplied sentinels (the most
// stable detection layer).
func classifyTypedError(err error) (bool, error) {
	if errors.Is(err, proxmox.ErrNotFound) {
		return true, fmt.Errorf("%w: %w", ErrVMNotFound, err)
	}
	if errors.Is(err, proxmox.ErrNotAuthorized) {
		return true, fmt.Errorf("%w: %w", ErrVMAccessDenied, err)
	}
	return false, nil
}

// classifyByHTTPStatus matches the HTTP status code embedded in the
// canonical "NNN Status Text" error format the library uses for
// unhandled 5xx responses (proxmox.go handleResponse).
func classifyByHTTPStatus(err error) (bool, error) {
	if httpStatusFromError(err) == http.StatusNotFound {
		return true, fmt.Errorf("%w: %w", ErrVMNotFound, err)
	}
	return false, nil
}

// classifyByMessage falls back to substring matches on the error body.
// Least preferred — kept because Proxmox returns 500s with the real
// failure encoded in the body and go-proxmox passes the body through.
func classifyByMessage(err error) (bool, error) {
	s := err.Error()
	switch {
	case strings.Contains(s, "does not exist"):
		return true, fmt.Errorf("%w: %w", ErrVMNotFound, err)
	case strings.Contains(s, "already running"):
		return true, fmt.Errorf("%w: %w", ErrVMAlreadyRunning, err)
	}
	return false, nil
}

// proxmoxErrorClassifiers is the ordered list of detection strategies.
// classifyProxmoxError walks the list and returns the first hit; the
// order encodes the priority documented on each classifier. Add a new
// detection layer by inserting a new entry — no rewrite of the
// dispatcher required.
var proxmoxErrorClassifiers = []errorClassifier{
	classifyTypedError,
	classifyByHTTPStatus,
	classifyByMessage,
}

// awaitTask waits for a Proxmox task to leave the running state and then
// inspects its terminal result. go-proxmox v0.7.0's Task.Wait/WaitFor
// returns nil the moment the task stops running — it never looks at
// IsFailed/ExitStatus — so a task that *completed with a failure*
// (storage-full clone, locked-VM destroy, failed start) would otherwise
// be reported as success. Ping (called inside Wait) populates IsFailed
// and ExitStatus, so we can check them once WaitFor returns. Use this in
// place of a bare task.WaitFor for every state-changing task.
func awaitTask(ctx context.Context, t *proxmox.Task, seconds int) error {
	if err := t.WaitFor(ctx, seconds); err != nil {
		return err
	}
	if t.IsFailed || (t.ExitStatus != "" && t.ExitStatus != "OK") {
		status := t.ExitStatus
		if status == "" {
			status = "unknown failure"
		}
		return fmt.Errorf("proxmox task %s failed: %s", t.UPID, status)
	}
	return nil
}

// classifyProxmoxError wraps err with our typed sentinels when the
// underlying go-proxmox / HTTP error matches a known operational
// condition. Returns the original error unchanged when no classifier
// recognises it — callers still see the original.
func classifyProxmoxError(err error) error {
	if err == nil {
		return nil
	}
	for _, c := range proxmoxErrorClassifiers {
		if ok, wrapped := c(err); ok {
			return wrapped
		}
	}
	return err
}

// httpStatusFromError extracts an HTTP status code from a go-proxmox
// error formatted as "%d Status Text" (e.g. "500 Internal Server
// Error", "404 Not Found"). Each error in the unwrap chain is inspected
// so a canonical status prefix survives contextual wrappers such as getVM's
// "get vm ..." prefix. Returns 0 if no recognisable status is present.
func httpStatusFromError(err error) int {
	for current := err; current != nil; current = errors.Unwrap(current) {
		s := current.Error()
		if len(s) < 3 || s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' || s[2] < '0' || s[2] > '9' {
			continue
		}
		if len(s) > 3 && s[3] != ' ' {
			continue
		}
		n, perr := strconv.Atoi(s[:3])
		if perr == nil && n >= 100 && n <= 599 {
			return n
		}
	}
	return 0
}

// isNotFound is kept as a thin adapter so internal call sites (Stop,
// Destroy, getVM) read naturally. Use the typed ErrVMNotFound externally.
func isNotFound(err error) bool {
	return errors.Is(classifyProxmoxError(err), ErrVMNotFound)
}

// isAlreadyRunning is the equivalent thin adapter for the "start an
// already-running VM" case.
func isAlreadyRunning(err error) bool {
	return errors.Is(classifyProxmoxError(err), ErrVMAlreadyRunning)
}

// encodeNIC renders a CloneNIC into Proxmox's net<idx> string
// syntax (e.g. "virtio,bridge=vmbr0,tag=42,mtu=9000"). Empty
// optional fields are omitted so Proxmox's defaults apply.
//
// Tag semantics:
//   - VLANUntagged == true     → no tag= attribute (untagged)
//   - VLANTag > 0              → tag=<N>
//   - VLANTag == 0 && !Untagged → no tag= attribute (use bridge default)
func encodeNIC(nic CloneNIC) string {
	model := nic.Model
	if model == "" {
		model = "virtio"
	}
	parts := []string{model, "bridge=" + nic.Bridge}
	if !nic.VLANUntagged && nic.VLANTag > 0 {
		parts = append(parts, fmt.Sprintf("tag=%d", nic.VLANTag))
	}
	if nic.MTU > 0 {
		parts = append(parts, fmt.Sprintf("mtu=%d", nic.MTU))
	}
	return strings.Join(parts, ",")
}
