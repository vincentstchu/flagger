package controller

import (
	"errors"
	"testing"
	"time"

	flaggerv1 "github.com/fluxcd/flagger/pkg/apis/flagger/v1beta1"
	"github.com/fluxcd/flagger/pkg/canary"
	fakeFlagger "github.com/fluxcd/flagger/pkg/client/clientset/versioned/fake"
	"github.com/fluxcd/flagger/pkg/router"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

type controlRouter struct {
	router.Interface
	fail bool
}

func (r controlRouter) SetRoutes(*flaggerv1.Canary, int, int, bool) error {
	if r.fail {
		return errors.New("route failure")
	}
	return nil
}

type controlDeployer struct {
	canary.Controller
	scale, status bool
}

func (d controlDeployer) ScaleToZero(*flaggerv1.Canary) error {
	if d.scale {
		return errors.New("scale failure")
	}
	return nil
}
func (d controlDeployer) SyncStatus(*flaggerv1.Canary, flaggerv1.CanaryStatus) error {
	if d.status {
		return errors.New("status failure")
	}
	return nil
}

type controlScaler struct {
	canary.ScalerReconciler
	fail bool
}

func (s controlScaler) PauseTargetScaler(*flaggerv1.Canary) error {
	if s.fail {
		return errors.New("scaler failure")
	}
	return nil
}

func pendingControlAge(t *testing.T, f fixture, operation string) float64 {
	t.Helper()
	r := prometheus.NewRegistry()
	r.MustRegister(f.ctrl.recorder.Control)
	ms, err := r.Gather()
	require.NoError(t, err)
	for _, m := range ms {
		if m.GetName() != "flagger_canary_pending_operation_age_seconds" {
			continue
		}
		for _, sample := range m.Metric {
			for _, label := range sample.Label {
				if label.GetName() == "operation" && label.GetValue() == operation {
					return sample.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatal("pending operation metric absent")
	return 0
}

func TestControlMetricsRollbackCompletion(t *testing.T) {
	for _, step := range []string{"route", "scaler", "scale", "status"} {
		t.Run(step, func(t *testing.T) {
			f := newDeploymentFixture(nil)
			f.ctrl.logger = zap.NewNop().Sugar()
			f.ctrl.recorder.Control.Enable()
			f.ctrl.rollback(f.canary, controlDeployer{scale: step == "scale", status: step == "status"}, controlRouter{fail: step == "route"}, controlScaler{fail: step == "scaler"})
			require.Greater(t, pendingControlAge(t, f, "rollback"), float64(0), "failed step cleared requested rollback")
			f.ctrl.rollback(f.canary, controlDeployer{}, controlRouter{}, controlScaler{})
			require.Zero(t, pendingControlAge(t, f, "rollback"), "successful rollback did not clear pending work")
		})
	}
}

func TestControlMetricsFinalizerRemoval(t *testing.T) {
	cd := newDeploymentTestCanary()
	cd.Spec.RevertOnDeletion = true
	cd.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	cd.Status.Phase = flaggerv1.CanaryPhaseTerminated
	cd.Finalizers = []string{finalizer}
	f := newDeploymentFixture(cd)
	f.ctrl.logger = zap.NewNop().Sugar()
	f.ctrl.recorder.Control.Enable()
	fail := true
	f.flaggerClient.(*fakeFlagger.Clientset).PrependReactor("update", "canaries", func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("finalizer update failure")
		}
		return false, nil, nil
	})
	require.Error(t, f.ctrl.syncHandler(cd.Namespace+"/"+cd.Name))
	require.Greater(t, pendingControlAge(t, f, "finalize"), float64(0))
	fail = false
	require.NoError(t, f.ctrl.syncHandler(cd.Namespace+"/"+cd.Name))
	require.Zero(t, pendingControlAge(t, f, "finalize"))
}
