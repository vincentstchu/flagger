package metrics

import (
	"sync"
	"time"

	flaggerv1 "github.com/fluxcd/flagger/pkg/apis/flagger/v1beta1"
	"github.com/prometheus/client_golang/prometheus"
)

// ControlCollector measures work, rather than exporter reachability. Revision
// and UID stay in memory and never become metric labels. Only the leader emits.
type ControlCollector struct {
	mu                                                             sync.Mutex
	now                                                            func() time.Time
	leader                                                         bool
	scheduled                                                      time.Time
	canaries                                                       map[string]*controlState
	scheduler, overdue, progressAge, budget, pendingAge, finalizer *prometheus.Desc
}

type controlState struct {
	uid, name, namespace, revision  string
	phase                           flaggerv1.CanaryPhase
	weight, iterations              int
	active, hasJob, hasRead         bool
	confirmed, finalizerConfirmed   bool
	observed, completed, progressed time.Time
	retiredUntil                    time.Time
	budget                          time.Duration
	operations                      map[string]time.Time
}

func newControlCollector(controller string) *ControlCollector {
	labels := []string{"canary_name", "canary_namespace"}
	desc := func(name, help string, labels []string) *prometheus.Desc {
		return prometheus.NewDesc(controller+"_"+name, help, labels, nil)
	}
	return &ControlCollector{
		now: time.Now, canaries: make(map[string]*controlState),
		scheduler:   desc("scheduler_age_seconds", "Seconds since the elected leader completed scheduling.", nil),
		overdue:     desc("canary_control_overdue", "One when a control call, active progress step or requested rollback is overdue; zero otherwise.", labels),
		progressAge: desc("canary_progress_age_seconds", "Seconds without a new active progress step, excluding explicit suspension and confirmation waits.", labels),
		budget:      desc("canary_progress_budget_seconds", "Per-step budget: max of three analysis intervals and the workload progress deadline.", labels),
		pendingAge:  desc("canary_pending_operation_age_seconds", "Seconds since a requested operation began without observed successful completion.", append(append([]string{}, labels...), "operation")),
		finalizer:   desc("canary_finalizer_overdue", "One when requested finalization has not completed within ten minutes; zero otherwise.", labels),
	}
}

func (cc *ControlCollector) Enable() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.leader = true
	cc.scheduled = cc.now()
}

func (cc *ControlCollector) Disable() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.leader = false
}

func (cc *ControlCollector) Scheduled() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.scheduled = cc.now()
}

func controlKey(cd *flaggerv1.Canary) string { return cd.Namespace + "/" + cd.Name }

func (cc *ControlCollector) state(cd *flaggerv1.Canary) *controlState {
	k := controlKey(cd)
	s := cc.canaries[k]
	if s == nil || s.uid != string(cd.UID) {
		now := cc.now()
		s = &controlState{uid: string(cd.UID), name: cd.Name, namespace: cd.Namespace,
			observed: now, completed: now, progressed: now, operations: make(map[string]time.Time)}
		cc.canaries[k] = s
	}
	// Reuse the configured single-workload deadline, never as a total release deadline.
	s.budget = 3 * cd.GetAnalysisInterval()
	if d := time.Duration(cd.GetProgressDeadlineSeconds()) * time.Second; d > s.budget {
		s.budget = d
	}
	return s
}

// Observe is called after a successful CR read at the beginning of each control
// call. Failed checks, timestamps refreshed by retries and stale analysis values
// do not reset the progress clock. A newly observed revision gets a grace budget.
func (cc *ControlCollector) Observe(cd *flaggerv1.Canary) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	s := cc.state(cd)
	now := cc.now()
	active := cd.DeletionTimestamp == nil && !cd.Spec.Suspend &&
		(cd.Status.ManualState == nil || !cd.Status.ManualState.Paused) &&
		(cd.Status.Phase == flaggerv1.CanaryPhaseProgressing ||
			cd.Status.Phase == flaggerv1.CanaryPhasePromoting || cd.Status.Phase == flaggerv1.CanaryPhaseFinalising)
	revision := cd.Status.LastAppliedSpec + "/" + cd.Status.LastStartTime.String()
	changed := revision != s.revision || s.phase != cd.Status.Phase ||
		s.weight != cd.Status.CanaryWeight || s.iterations != cd.Status.Iterations
	if !active || !s.active || changed {
		s.progressed = now
	}
	if !active || (s.hasRead && changed) {
		s.confirmed = true
	}
	if cd.DeletionTimestamp == nil && s.operations["finalize"].IsZero() {
		s.finalizerConfirmed = true
	}
	s.active, s.revision, s.phase = active, revision, cd.Status.Phase
	s.hasJob = true
	s.hasRead = true
	s.weight, s.iterations, s.observed = cd.Status.CanaryWeight, cd.Status.Iterations, now
}

// Expect establishes target liveness even when the first live CR GET fails.
// Cached informer state is not accepted as fresh control evidence.
func (cc *ControlCollector) Expect(cd *flaggerv1.Canary) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.state(cd).hasJob = true
}

func (cc *ControlCollector) Completed(cd *flaggerv1.Canary) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if s := cc.canaries[controlKey(cd)]; s != nil && s.uid == string(cd.UID) {
		s.completed = cc.now()
	}
}

// StartOperation does not restart the clock on retries. CompleteOperation must
// only follow all route/scale/status steps, or finalizer removal, succeeding.
func (cc *ControlCollector) StartOperation(cd *flaggerv1.Canary, operation string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	s := cc.state(cd)
	if _, exists := s.operations[operation]; !exists {
		s.operations[operation] = cc.now()
	}
}

func (cc *ControlCollector) CompleteOperation(cd *flaggerv1.Canary, operation string) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if s := cc.canaries[controlKey(cd)]; s != nil && s.uid == string(cd.UID) {
		delete(s.operations, operation)
		if operation == "rollback" || operation == "start" {
			s.confirmed = true
		}
		if operation == "finalize" {
			s.finalizerConfirmed = true
		}
	}
}

func (cc *ControlCollector) Forget(cd *flaggerv1.Canary) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if s := cc.canaries[controlKey(cd)]; s != nil && s.uid == string(cd.UID) {
		if s.finalizerConfirmed && len(s.operations) == 0 {
			// Keep observed successful completion scrapeable after the CR is gone.
			s.retiredUntil = cc.now().Add(10 * time.Minute)
			s.active, s.hasJob, s.confirmed = false, false, true
		} else {
			delete(cc.canaries, controlKey(cd))
		}
	}
}

func (cc *ControlCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{cc.scheduler, cc.overdue, cc.progressAge, cc.budget, cc.pendingAge, cc.finalizer} {
		ch <- d
	}
}

func age(now, then time.Time) float64 {
	if then.IsZero() || now.Before(then) {
		return 0
	}
	return now.Sub(then).Seconds()
}

func (cc *ControlCollector) Collect(ch chan<- prometheus.Metric) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if !cc.leader {
		return
	}
	now := cc.now()
	ch <- prometheus.MustNewConstMetric(cc.scheduler, prometheus.GaugeValue, age(now, cc.scheduled))
	for key, s := range cc.canaries {
		if !s.retiredUntil.IsZero() && !now.Before(s.retiredUntil) {
			delete(cc.canaries, key)
			continue
		}
		labels := []string{s.name, s.namespace}
		progress := 0.0
		if s.active {
			progress = age(now, s.progressed)
		}
		overdue := 0.0
		// A completed read-only/idle call is still evidence that this job can run.
		// This is deliberately distinct from advancing an active release step.
		if (s.hasJob && age(now, s.completed) > 600) || progress > s.budget.Seconds() || age(now, s.operations["rollback"]) > 600 || age(now, s.operations["start"]) > 600 {
			overdue = 1
		}
		// After restart/failover, an active unchanged revision is Unknown until
		// fresh progress/completion is observed or its budget expires. Resetting
		// this process's clock must not emit an unearned recovery zero.
		if s.confirmed || overdue == 1 {
			ch <- prometheus.MustNewConstMetric(cc.overdue, prometheus.GaugeValue, overdue, labels...)
		}
		ch <- prometheus.MustNewConstMetric(cc.progressAge, prometheus.GaugeValue, progress, labels...)
		ch <- prometheus.MustNewConstMetric(cc.budget, prometheus.GaugeValue, s.budget.Seconds(), labels...)
		for _, operation := range []string{"start", "rollback", "finalize"} {
			ch <- prometheus.MustNewConstMetric(cc.pendingAge, prometheus.GaugeValue, age(now, s.operations[operation]), append(append([]string{}, labels...), operation)...)
		}
		finalizer := 0.0
		if age(now, s.operations["finalize"]) > 600 {
			finalizer = 1
		}
		if s.finalizerConfirmed || finalizer == 1 {
			ch <- prometheus.MustNewConstMetric(cc.finalizer, prometheus.GaugeValue, finalizer, labels...)
		}
	}
}
