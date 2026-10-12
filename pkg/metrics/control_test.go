package metrics

import (
	"strings"
	"testing"
	"time"

	flaggerv1 "github.com/fluxcd/flagger/pkg/apis/flagger/v1beta1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func replayCollector() (*ControlCollector, *flaggerv1.Canary, *time.Time) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	cc := newControlCollector("flagger")
	cc.now = func() time.Time { return now }
	cd := &flaggerv1.Canary{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns-staging", UID: types.UID("uid-1")}}
	cd.Spec.Analysis = &flaggerv1.CanaryAnalysis{Interval: "5m"}
	cd.Status.Phase = flaggerv1.CanaryPhaseProgressing
	cd.Status.LastAppliedSpec = "revision-in-memory"
	cc.Enable()
	cc.Scheduled()
	cc.Observe(cd)
	return cc, cd, &now
}

func metric(t *testing.T, cc *ControlCollector, name string) float64 {
	t.Helper()
	r := prometheus.NewRegistry()
	r.MustRegister(cc)
	ms, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if m.GetName() == name {
			return m.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s absent", name)
	return 0
}

func TestControlReplayProgressAndProviderRetries(t *testing.T) {
	cc, cd, now := replayCollector()
	// A forty-minute release with a step every five minutes is healthy.
	for i := 1; i <= 8; i++ {
		*now = now.Add(5 * time.Minute)
		cd.Status.Iterations = i
		cc.Observe(cd)
		cc.Completed(cd)
		if got := metric(t, cc, "flagger_canary_control_overdue"); got != 0 {
			t.Fatal("normal release classified overdue")
		}
	}
	// Retrying 429/empty-provider checks and refreshed transition timestamps
	// do not pretend to advance a step, even if a failed-check counter changes.
	for i := 0; i < 31; i++ {
		*now = now.Add(30 * time.Second)
		cd.Status.FailedChecks++
		cd.Status.LastTransitionTime = metav1.NewTime(*now)
		cc.Observe(cd)
		cc.Completed(cd)
	}
	if got := metric(t, cc, "flagger_canary_control_overdue"); got != 1 {
		t.Fatal("provider retry stall missed")
	}
	cd.Status.CanaryWeight++
	cc.Observe(cd)
	cc.Completed(cd)
	if got := metric(t, cc, "flagger_canary_control_overdue"); got != 0 {
		t.Fatal("progress recovery missed")
	}
}

func TestControlReplayLegitimateWaitsAndIdle(t *testing.T) {
	for _, phase := range []flaggerv1.CanaryPhase{flaggerv1.CanaryPhaseInitializing, flaggerv1.CanaryPhaseInitialized,
		flaggerv1.CanaryPhaseSucceeded, flaggerv1.CanaryPhaseFailed, flaggerv1.CanaryPhaseWaiting, flaggerv1.CanaryPhaseWaitingPromotion} {
		t.Run(string(phase), func(t *testing.T) {
			cc, cd, now := replayCollector()
			cd.Status.Phase = phase
			cc.Observe(cd)
			*now = now.Add(24 * time.Hour)
			cc.Observe(cd)
			cc.Completed(cd)
			if got := metric(t, cc, "flagger_canary_control_overdue"); got != 0 {
				t.Fatal("idle/manual gate classified as failure")
			}
		})
	}
	for _, suspend := range []bool{false, true} {
		cc, cd, now := replayCollector()
		cd.Spec.Suspend = suspend
		cd.Status.ManualState = &flaggerv1.CanaryManualState{Paused: !suspend}
		cc.Observe(cd)
		*now = now.Add(time.Hour)
		cc.Observe(cd)
		cc.Completed(cd)
		if got := metric(t, cc, "flagger_canary_control_overdue"); got != 0 {
			t.Fatal("explicit hold classified as failure")
		}
		cd.Spec.Suspend = false
		cd.Status.ManualState.Paused = false
		cc.Observe(cd)
		cc.Completed(cd)
		if got := metric(t, cc, "flagger_canary_progress_age_seconds"); got != 0 {
			t.Fatal("manual wait charged to resumed step")
		}
	}
}

func TestControlReplayHungJobAndLeader(t *testing.T) {
	cc, cd, now := replayCollector()
	cc.Completed(cd)
	*now = now.Add(601 * time.Second)
	if got := metric(t, cc, "flagger_canary_control_overdue"); got != 1 {
		t.Fatal("hung job missed")
	}
	cc.Observe(cd)
	cc.Completed(cd)
	cd.Status.CanaryWeight++
	cc.Observe(cd)
	if got := metric(t, cc, "flagger_canary_control_overdue"); got != 0 {
		t.Fatal("job recovery missed")
	}
	if got := metric(t, cc, "flagger_scheduler_age_seconds"); got != 601 {
		t.Fatal("scheduler clock did not age independently of scraping")
	}
	cc.Scheduled()
	if got := metric(t, cc, "flagger_scheduler_age_seconds"); got != 0 {
		t.Fatal("scheduler recovery missed")
	}
	cc.Disable()
	if got := testutil.CollectAndCount(cc); got != 0 {
		t.Fatal("standby/former leader emitted stale control state")
	}
}

func TestControlReplayFirstReadFailure(t *testing.T) {
	cc, cd, now := replayCollector()
	cc.canaries = make(map[string]*controlState)
	cc.Expect(cd)
	*now = now.Add(601 * time.Second)
	if got := metric(t, cc, "flagger_canary_control_overdue"); got != 1 {
		t.Fatal("expected job with failing first live GET was not detected")
	}
	cd.Status.Phase = flaggerv1.CanaryPhaseSucceeded
	cc.Observe(cd)
	cc.Completed(cd)
	if got := metric(t, cc, "flagger_canary_control_overdue"); got != 0 {
		t.Fatal("fresh successful idle read did not recover job liveness")
	}
}

func TestControlReplayOperationCompletionAndLifecycle(t *testing.T) {
	for _, op := range []string{"start", "rollback", "finalize"} {
		t.Run(op, func(t *testing.T) {
			cc, cd, now := replayCollector()
			cd.Status.Phase = flaggerv1.CanaryPhaseSucceeded
			cc.Observe(cd)
			cc.StartOperation(cd, op)
			*now = now.Add(601 * time.Second)
			cc.StartOperation(cd, op)
			cc.Observe(cd)
			cc.Completed(cd)
			name := "flagger_canary_control_overdue"
			if op == "finalize" {
				name = "flagger_canary_finalizer_overdue"
			}
			if got := metric(t, cc, name); got != 1 {
				t.Fatal("retry reset pending operation clock")
			}
			// A new revision or an idle/Failed phase cannot prove the old action completed.
			cd.Status.LastAppliedSpec = "next-revision"
			cc.Observe(cd)
			cc.Completed(cd)
			if got := metric(t, cc, name); got != 1 {
				t.Fatal("revision change cleared unresolved operation")
			}
			cc.CompleteOperation(cd, op)
			if got := metric(t, cc, name); got != 0 {
				t.Fatal("successful operation did not clear pending state")
			}
		})
	}
	cc, cd, _ := replayCollector()
	cc.StartOperation(cd, "finalize")
	cc.Forget(cd)
	if got := testutil.CollectAndCount(cc); got != 1 {
		t.Fatal("deleted CR left target series")
	}
	cd.UID = types.UID("uid-2")
	cc.Observe(cd)
	if got := metric(t, cc, "flagger_canary_progress_age_seconds"); got != 0 {
		t.Fatal("new UID reused old progress age")
	}
	r := prometheus.NewRegistry()
	r.MustRegister(cc)
	ms, _ := r.Gather()
	for _, m := range ms {
		for _, sample := range m.Metric {
			for _, label := range sample.Label {
				if strings.Contains(label.GetValue(), "revision") || strings.Contains(label.GetValue(), "uid-") {
					t.Fatal("revision/UID leaked into labels")
				}
			}
		}
	}
}

func TestControlReplayRestartAndFinalizerRecoveryRetention(t *testing.T) {
	cc, cd, now := replayCollector()
	r := prometheus.NewRegistry()
	r.MustRegister(cc)
	has := func(name string) bool {
		ms, err := r.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range ms {
			if m.GetName() == name {
				return true
			}
		}
		return false
	}
	if has("flagger_canary_control_overdue") {
		t.Fatal("cold active revision emitted a recovery zero before fresh progress")
	}
	cd.DeletionTimestamp = &metav1.Time{Time: *now}
	// Use a fresh collector to simulate a new leader observing an old deletion.
	cc.canaries = make(map[string]*controlState)
	cc.StartOperation(cd, "finalize")
	if has("flagger_canary_finalizer_overdue") {
		t.Fatal("pending finalizer after restart emitted an unearned zero")
	}
	*now = now.Add(601 * time.Second)
	if got := metric(t, cc, "flagger_canary_finalizer_overdue"); got != 1 {
		t.Fatal("finalizer timeout missed")
	}
	cc.CompleteOperation(cd, "finalize")
	cc.Forget(cd)
	if got := metric(t, cc, "flagger_canary_finalizer_overdue"); got != 0 {
		t.Fatal("completed finalizer zero was not retained for scraping")
	}
	*now = now.Add(10 * time.Minute)
	if has("flagger_canary_finalizer_overdue") {
		t.Fatal("retired CR retained unbounded metric history")
	}
}

func TestControlReplayPendingAndColdScheduling(t *testing.T) {
	cc, cd, now := replayCollector()
	cd.Status.Phase = flaggerv1.CanaryPhaseSucceeded
	cc.Observe(cd)
	r := prometheus.NewRegistry()
	r.MustRegister(cc)
	has := func(name string) bool {
		ms, err := r.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range ms {
			if m.GetName() == name {
				return true
			}
		}
		return false
	}
	for _, op := range []string{"start", "rollback", "finalize"} {
		name := "flagger_canary_control_overdue"
		if op == "finalize" {
			name = "flagger_canary_finalizer_overdue"
		}
		cc.StartOperation(cd, op)
		cc.Observe(cd)
		cc.Completed(cd)
		if has(name) {
			t.Fatal("unfinished action emitted a healthy zero")
		}
		cc.CompleteOperation(cd, op)
		if got := metric(t, cc, name); got != 0 {
			t.Fatal("completion did not emit recovery")
		}
	}
	cc.Disable()
	cc.Enable()
	if has("flagger_scheduler_age_seconds") || has("flagger_canary_control_overdue") {
		t.Fatal("new leader emitted stale or unearned work health")
	}
	*now = now.Add(601 * time.Second)
	if has("flagger_scheduler_age_seconds") {
		t.Fatal("unscheduled leader fabricated completion")
	}
	cc.Scheduled()
	if got := metric(t, cc, "flagger_scheduler_age_seconds"); got != 0 {
		t.Fatal("real scheduling did not emit health")
	}
}
