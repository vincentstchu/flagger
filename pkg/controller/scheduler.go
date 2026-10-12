/*
Copyright 2020 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"github.com/fluxcd/flagger/pkg/metrics/providers"
	"go.uber.org/zap/zapcore"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	flaggerv1 "github.com/fluxcd/flagger/pkg/apis/flagger/v1beta1"
	"github.com/fluxcd/flagger/pkg/canary"
	"github.com/fluxcd/flagger/pkg/router"
)

func (c *Controller) min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *Controller) maxWeight(canary *flaggerv1.Canary) int {
	var stepWeightsLen = len(canary.GetAnalysis().StepWeights)
	if stepWeightsLen > 0 {
		return c.min(c.totalWeight(canary), canary.GetAnalysis().StepWeights[stepWeightsLen-1])
	}
	if canary.GetAnalysis().MaxWeight > 0 {
		return canary.GetAnalysis().MaxWeight
	}
	// set max weight default value to total weight
	return c.totalWeight(canary)
}

func (c *Controller) totalWeight(canary *flaggerv1.Canary) int {
	// set total weight default value to 100%
	return 100
}

func (c *Controller) nextStepWeight(canary *flaggerv1.Canary, canaryWeight int) int {
	var stepWeightsLen = len(canary.GetAnalysis().StepWeights)
	if canary.GetAnalysis().StepWeight > 0 || stepWeightsLen == 0 {
		return canary.GetAnalysis().StepWeight
	}

	totalWeight := c.totalWeight(canary)
	maxStep := totalWeight - canaryWeight

	// If maxStep is zero we need to promote, so any non zero step weight will move the canary to promotion.
	// This is the same use case as the last step via StepWeight.
	if maxStep == 0 {
		return 1
	}

	// return min of maxStep and the calculated step to avoid going above totalWeight

	// initial step
	if canaryWeight == 0 {
		return c.min(maxStep, canary.GetAnalysis().StepWeights[0])
	}

	// find the current step and return the difference in weight
	for i := 0; i < stepWeightsLen-1; i++ {
		if canary.GetAnalysis().StepWeights[i] == canaryWeight {
			return c.min(maxStep, canary.GetAnalysis().StepWeights[i+1]-canaryWeight)
		}
	}

	return maxStep
}

// scheduleCanaries synchronises the canary map with the jobs map,
// for new canaries new jobs are created and started
// for the removed canaries the jobs are stopped and deleted
func (c *Controller) scheduleCanaries() {
	defer c.recorder.Control.Scheduled()
	current := make(map[string]string)
	stats := make(map[string]int)

	c.canaries.Range(func(key interface{}, value interface{}) bool {
		cn := value.(*flaggerv1.Canary)

		// format: <name>.<namespace>
		name := key.(string)
		current[name] = fmt.Sprintf("%s.%s", cn.Spec.TargetRef.Name, cn.Namespace)

		job, exists := c.jobs[name]
		// get analysis interval
		analysisInterval := cn.GetAnalysisInterval()
		// schedule new job for existing job with different analysis interval or non-existing job
		if (exists && job.GetCanaryAnalysisInterval() != analysisInterval) || !exists {
			if exists {
				job.Stop()
			} else {
				// Avoid starting without metrics.
				c.recorderMetrics(cn)
			}

			newJob := CanaryJob{
				Name:             cn.Name,
				Namespace:        cn.Namespace,
				function:         c.advanceCanary,
				done:             make(chan bool),
				ticker:           time.NewTicker(getAnalysisInterval(cn)), // max to 30s
				analysisInterval: analysisInterval,
			}

			c.recorder.Control.Expect(cn)
			c.jobs[name] = newJob
			newJob.Start()
		}

		// compute canaries per namespace total
		t, ok := stats[cn.Namespace]
		if !ok {
			stats[cn.Namespace] = 1
		} else {
			stats[cn.Namespace] = t + 1
		}
		return true
	})

	// cleanup deleted jobs
	for job := range c.jobs {
		if _, exists := current[job]; !exists {
			c.jobs[job].Stop()
			delete(c.jobs, job)
		}
	}

	// check if multiple canaries have the same target
	for canaryName, targetName := range current {
		for name, target := range current {
			if name != canaryName && target == targetName {
				c.logger.With("canary", canaryName).
					Errorf("Bad things will happen! Found more than one canary with the same target %s", targetName)
			}
		}
	}

	// set total canaries per namespace metric
	for k, v := range stats {
		c.recorder.SetTotal(k, v)
	}
}

func getAnalysisInterval(cn *flaggerv1.Canary) time.Duration {
	if cn.GetAnalysisInterval() > time.Second*30 {
		return time.Second * 30
	}
	return cn.GetAnalysisInterval()
}

func (c *Controller) advanceCanary(name string, namespace string) {
	begin := time.Now()
	// check if the canary exists
	cd, err := c.flaggerClient.FlaggerV1beta1().Canaries(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		c.logger.With("canary", fmt.Sprintf("%s.%s", name, namespace)).
			Errorf("Canary %s.%s not found", name, namespace)
		return
	}

	c.recorder.Control.Observe(cd)
	defer c.recorder.Control.Completed(cd)
	if cd.Spec.Suspend {
		msg := "skipping canary run as object is suspended"
		c.logger.With("canary", fmt.Sprintf("%s.%s", name, namespace)).
			Debug(msg)
		c.recordEventInfof(cd, "%s", msg)
		return
	}

	// override the global provider if one is specified in the canary spec
	provider := c.meshProvider
	if cd.Spec.Provider != "" {
		provider = cd.Spec.Provider
	}

	// init controller based on target kind
	canaryController := c.canaryFactory.Controller(cd.Spec.TargetRef.Kind)

	labelSelector, labelValue, ports, _, err := canaryController.GetMetadata(cd)
	if err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}

	var scalerReconciler canary.ScalerReconciler
	if cd.Spec.AutoscalerRef != nil {
		scalerReconciler = c.canaryFactory.ScalerReconciler(cd.Spec.AutoscalerRef.Kind)
	}

	// init Kubernetes router
	kubeRouter := c.routerFactory.KubernetesRouter(cd.Spec.TargetRef.Kind, labelSelector, labelValue, ports)

	// reconcile the canary/primary services
	if err := kubeRouter.Initialize(cd); err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}

	// check metric servers' availability
	if !cd.SkipAnalysis() && (cd.Status.Phase == "" || cd.Status.Phase == flaggerv1.CanaryPhaseInitializing) {
		if err := c.checkMetricProviderAvailability(cd); err != nil {
			c.recordEventErrorf(cd, "Error checking metric providers: %v", err)
		}
	}

	// init mesh router
	meshRouter := c.routerFactory.MeshRouter(provider, labelSelector)

	// register the AppMesh VirtualNodes before creating the primary deployment
	// otherwise the pods will not be injected with the Envoy proxy
	if strings.HasPrefix(provider, flaggerv1.AppMeshProvider) {
		if err := meshRouter.Reconcile(cd); err != nil {
			c.recordEventWarningf(cd, "%v", err)
			return
		}
	}

	// create primary workload
	retriable, err := canaryController.Initialize(cd)
	if err != nil {
		c.recordEventWarningf(cd, "%v", err)
		if !retriable {
			c.recordEventWarningf(cd, "Rolling back %s.%s: Progress deadline exceeded. Primary workload creation failed: %v",
				cd.Name, cd.Namespace, err)

			c.alert(cd, fmt.Sprintf("Rolling back %s.%s: Progress deadline exceeded. Primary workload creation failed: %v", cd.Name, cd.Namespace, err),
				false, flaggerv1.SeverityError)

			c.rollback(cd, canaryController, meshRouter, scalerReconciler)
		}
		return
	}

	if scalerReconciler != nil {
		err = scalerReconciler.ReconcilePrimaryScaler(cd, true)
		if err != nil {
			c.recordEventWarningf(cd, "%v", err)
			return
		}
		if cd.Status.Phase == "" || cd.Status.Phase == flaggerv1.CanaryPhaseInitializing {
			err = scalerReconciler.PauseTargetScaler(cd)
			if err != nil {
				c.recordEventWarningf(cd, "%v", err)
				return
			}
		}
	}

	// change the apex service pod selector to primary
	if err := kubeRouter.Reconcile(cd); err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}

	// scale down the canary target to 0 replicas after the service is pointing to the primary target
	if cd.Status.Phase == "" || cd.Status.Phase == flaggerv1.CanaryPhaseInitializing {
		c.logCanaryEvent(cd, fmt.Sprintf("Scaling down %s %s.%s", cd.Spec.TargetRef.Kind, cd.Spec.TargetRef.Name, cd.Namespace), zapcore.InfoLevel)
		if err := canaryController.ScaleToZero(cd); err != nil {
			c.recordEventWarningf(cd, "scaling down canary %s %s.%s failed: %v", cd.Spec.TargetRef.Kind, cd.Spec.TargetRef.Name, cd.Namespace, err)
			return
		}
	}

	// take over an existing virtual service or ingress
	// runs after the primary is ready to ensure zero downtime
	if !strings.HasPrefix(provider, flaggerv1.AppMeshProvider) {
		if err := meshRouter.Reconcile(cd); err != nil {
			c.recordEventWarningf(cd, "%v", err)
			return
		}
	}

	// set canary phase to initialized and sync the status
	if err = c.setPhaseInitialized(cd, canaryController); err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}

	// check for changes
	shouldAdvance, err := c.shouldAdvance(cd, canaryController)
	if err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}

	if !shouldAdvance {
		c.recorder.SetStatus(cd, cd.Status.Phase)
		return
	}

	maxWeight := c.maxWeight(cd)

	// check primary status
	if !cd.SkipAnalysis() {
		retriable, err := canaryController.IsPrimaryReady(cd)
		if err != nil {
			c.recordEventWarningf(cd, "Primary workload readiness check failed: %v", err)
			if !retriable {
				c.recordEventWarningf(cd, "Rolling back %s.%s: Progress deadline exceeded. Primary workload is not ready: %v",
					cd.Name, cd.Namespace, err)
				c.alert(cd, fmt.Sprintf("Rolling back %s.%s: Progress deadline exceeded. Primary workload is not ready: %v", cd.Name, cd.Namespace, err),
					false, flaggerv1.SeverityError)
				c.rollback(cd, canaryController, meshRouter, scalerReconciler)
			}
			return
		}
	}

	// get the routing settings
	primaryWeight, canaryWeight, mirrored, err := meshRouter.GetRoutes(cd)
	if err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}

	c.recorder.SetWeight(cd, primaryWeight, canaryWeight)

	// check if canary analysis should start (canary revision has changes) or continue
	if ok := c.checkCanaryStatus(cd, canaryController, scalerReconciler, shouldAdvance); !ok {
		return
	}

	// check if canary revision changed during analysis
	if restart := c.hasCanaryRevisionChanged(cd, canaryController); restart {
		c.logCanaryEvent(cd, "Canary revision changed during analysis, restarting analysis", zapcore.InfoLevel)
		// route all traffic back to primary
		primaryWeight = c.totalWeight(cd)
		canaryWeight = 0
		if err := meshRouter.SetRoutes(cd, primaryWeight, canaryWeight, false); err != nil {
			c.recordEventWarningf(cd, "%v", err)
			return
		}

		// reset status
		status := flaggerv1.CanaryStatus{
			Phase:         flaggerv1.CanaryPhaseProgressing,
			CanaryWeight:  0,
			FailedChecks:  0,
			Iterations:    0,
			LastStartTime: metav1.Now(),
		}
		if err := canaryController.SyncStatus(cd, status); err != nil {
			c.recordEventWarningf(cd, "%v", err)
			return
		}
		cd, err = c.flaggerClient.FlaggerV1beta1().Canaries(cd.Namespace).Get(context.TODO(), cd.Name, metav1.GetOptions{})
		if err != nil {
			c.logger.With("canary", fmt.Sprintf("%s.%s", cd.Name, cd.Namespace)).
				With("canary_name", cd.Name).
				With("canary_namespace", cd.Namespace).Errorf("%v", err)
			return
		}
		c.recordEventWarningf(cd, "New revision detected! Restarting Canary analysis for %s.%s",
			cd.Spec.TargetRef.Name, cd.Namespace)
		// send alert
		c.alert(cd, fmt.Sprintf("New revision detected! Restarting Canary analysis for %s.%s",
			cd.Spec.TargetRef.Name, cd.Namespace),
			true, flaggerv1.SeverityWarn)
		return
	}

	// check canary status
	retriable, err = canaryController.IsCanaryReady(cd)
	if err != nil {
		c.recordEventWarningf(cd, "Error checking canary workload status: %v", err)
		if !retriable {
			c.recordEventWarningf(cd, "Rolling back %s.%s: Error checking canary workload status: %v",
				cd.Name, cd.Namespace, err)
			c.alert(cd, fmt.Sprintf("Rolling back %s.%s: Canary Progress deadline exceeded. Error checking canary workload status: %v", cd.Name, cd.Namespace, err),
				false, flaggerv1.SeverityError)
			c.rollback(cd, canaryController, meshRouter, scalerReconciler)
		}
		return
	}

	// check if analysis should be skipped
	if skip := c.shouldSkipAnalysis(cd, canaryController, meshRouter, scalerReconciler, err, retriable); skip {
		return
	}

	// check if we should rollback
	if cd.Status.Phase == flaggerv1.CanaryPhaseProgressing ||
		cd.Status.Phase == flaggerv1.CanaryPhaseWaiting ||
		cd.Status.Phase == flaggerv1.CanaryPhaseWaitingPromotion {
		if ok := c.runRollbackHooks(cd, cd.Status.Phase); ok {
			c.recordEventWarningf(cd, "Rolling back %s.%s manual webhook invoked", cd.Name, cd.Namespace)
			c.alert(cd, fmt.Sprintf("Rolling back  %s.%s manual webhook invoked", cd.Name, cd.Namespace), false, flaggerv1.SeverityWarn)
			c.rollback(cd, canaryController, meshRouter, scalerReconciler)
			return
		}
	}

	// route traffic back to primary if analysis has succeeded
	if cd.Status.Phase == flaggerv1.CanaryPhasePromoting {
		if scalerReconciler != nil {
			if err := scalerReconciler.ReconcilePrimaryScaler(cd, false); err != nil {
				c.recordEventWarningf(cd, "%v", err)
				return
			}
		}
		c.runPromotionTrafficShift(cd, canaryController, meshRouter, provider, canaryWeight, primaryWeight)
		return
	}

	// scale canary to zero if promotion has finished
	if cd.Status.Phase == flaggerv1.CanaryPhaseFinalising {
		if scalerReconciler != nil {
			if err := scalerReconciler.PauseTargetScaler(cd); err != nil {
				c.recordEventWarningf(cd, "%v", err)
				return
			}
		}
		if err := canaryController.ScaleToZero(cd); err != nil {
			c.recordEventWarningf(cd, "%v", err)
			return
		}

		// set status to succeeded
		if err := canaryController.SetStatusPhase(cd, flaggerv1.CanaryPhaseSucceeded); err != nil {
			c.recordEventWarningf(cd, "%v", err)
			return
		}
		c.recorder.SetStatus(cd, flaggerv1.CanaryPhaseSucceeded)
		c.runPostRolloutHooks(cd, flaggerv1.CanaryPhaseSucceeded)
		c.recordEventInfof(cd, "Promotion completed! Scaling down %s.%s", cd.Spec.TargetRef.Name, cd.Namespace)
		c.alert(cd, "Canary analysis completed successfully, promotion finished.",
			false, flaggerv1.SeveritySuccess)
		return
	}

	// check if the number of failed checks reached the threshold
	if (cd.Status.Phase == flaggerv1.CanaryPhaseProgressing || cd.Status.Phase == flaggerv1.CanaryPhaseWaitingPromotion) &&
		(!retriable || cd.Status.FailedChecks >= cd.GetAnalysisThreshold()) {
		if !retriable {
			c.recordEventWarningf(cd, "Rolling back %s.%s progress deadline exceeded %v",
				cd.Name, cd.Namespace, err)
			c.alert(cd, fmt.Sprintf("Rolling back %s.%s . Progress deadline exceeded and failed checks exceeded threshold: %v", cd.Name, cd.Namespace, err),
				false, flaggerv1.SeverityError)
		}
		c.rollback(cd, canaryController, meshRouter, scalerReconciler)
		return
	}

	// record analysis duration
	defer func() {
		c.recorder.SetDuration(cd, time.Since(begin))
	}()

	// check if the canary success rate is above the threshold
	// skip check if no traffic is routed or mirrored to canary
	if canaryWeight == 0 && cd.Status.Iterations == 0 &&
		!(cd.GetAnalysis().Mirror && mirrored) {
		c.recordEventInfof(cd, "Starting canary analysis for %s.%s", cd.Spec.TargetRef.Name, cd.Namespace)

		// run pre-rollout web hooks
		if ok := c.runPreRolloutHooks(cd); !ok {
			if err := canaryController.SetStatusFailedChecks(cd, cd.Status.FailedChecks+1); err != nil {
				c.recordEventWarningf(cd, "%v", err)
			} else {
				c.recordEventInfof(cd, "Pre-rollout webhooks error")
			}
			return
		}
	} else {
		if ok, err := c.runAnalysis(cd); !ok {
			// Handle manual status even when analysis fails
			// This ensures manual webhooks are processed even when metrics fail
			if shouldSkipRunCanary, err := c.handleManualStatus(cd, canaryController, meshRouter); err != nil {
				c.recordEventWarningf(cd, "Failed to handle manual status: %v", err)
				return
			} else if shouldSkipRunCanary {
				return
			}

			//  skip analysis
			if errors.Is(err, providers.ErrSkipAnalysis) {
				if skip := c.shouldSkipAnalysis(cd, canaryController, meshRouter, scalerReconciler, err, retriable); skip {
					return
				}
			}
			//  retriable errors
			if errors.Is(err, providers.ErrTooManyRequests) {
				return
			}
			if err := canaryController.SetStatusFailedChecks(cd, cd.Status.FailedChecks+1); err != nil {
				c.recordEventWarningf(cd, "%v", err)
			} else {
				c.recordEventWarningf(cd, "Analysis failed: %v", err)
			}
			return
		}
	}

	// use blue/green strategy for kubernetes provider
	if provider == flaggerv1.KubernetesProvider {
		if len(cd.GetAnalysis().Match) > 0 {
			c.recordEventWarningf(cd, "A/B testing is not supported when using the kubernetes provider")
			cd.GetAnalysis().Match = nil
		}
		if cd.GetAnalysis().Iterations < 1 {
			c.recordEventWarningf(cd, "Progressive traffic is not supported when using the kubernetes provider")
			c.recordEventWarningf(cd, "Setting canaryAnalysis.iterations: 10")
			cd.GetAnalysis().Iterations = 10
		}
	}

	// 检查是否到达执行Canary的时间,否则返回
	if cd.Status.LastTransitionTime.Add(cd.GetAnalysisInterval()).After(time.Now()) {
		return
	}

	// strategy: A/B testing
	if len(cd.GetAnalysis().Match) > 0 && cd.GetAnalysis().Iterations > 0 {
		c.runAB(cd, canaryController, meshRouter)
		return
	}

	// strategy: Blue/Green
	if cd.GetAnalysis().Iterations > 0 {
		c.runBlueGreen(cd, canaryController, meshRouter, provider, mirrored)
		return
	}

	// strategy: Canary progressive traffic increase
	if c.nextStepWeight(cd, canaryWeight) > 0 {
		// handle manual canary controls
		if shouldSkipRunCanary, err := c.
			handleManualStatus(cd, canaryController, meshRouter); err != nil {
			c.recordEventWarningf(cd, "Failed to handle manual status: %v", err)
			return
		} else if shouldSkipRunCanary {
			return
		}

		// run hook only if traffic is not mirrored
		if !mirrored &&
			(cd.Status.Phase != flaggerv1.CanaryPhasePromoting &&
				cd.Status.Phase != flaggerv1.CanaryPhaseWaitingPromotion &&
				cd.Status.Phase != flaggerv1.CanaryPhaseFinalising) {
			if promote := c.runConfirmTrafficIncreaseHooks(cd); !promote {
				return
			}
		}

		c.runCanary(cd, canaryController, meshRouter, mirrored, canaryWeight, primaryWeight, maxWeight)
	}

}

// handleManualStatus checks for manual intervention commands from webhooks and applies them.
// It returns true if the canary progression should be paused, and true if normal flow should be overridden.
func (c *Controller) handleManualStatus(canary *flaggerv1.Canary, canaryController canary.Controller, meshRouter router.Interface) (bool, error) {
	manualState, err := c.runManualTrafficControlHooks(canary)
	if err != nil {
		return false, fmt.Errorf("runManualTrafficControlHooks failed: %w", err)
	}

	// if manual state is not configured, resume
	if manualState == nil || manualState.Timestamp == "" {
		if canary.Status.ManualState != nil {
			return canary.Status.ManualState.Paused, nil
		}
		return false, nil
	}

	// compare timestamps to see if this is a new command
	// Also check if the manual state content has changed (for cases where same timestamp but different content)
	// Parse timestamps as integers to ensure proper numerical comparison
	manualTimestamp := manualState.Timestamp
	lastAppliedTimestamp := canary.Status.LastAppliedManualTimestamp

	// If we can parse both as integers (unix timestamps), do numerical comparison, otherwise do string comparison
	manualTs, manualErr := strconv.Atoi(manualTimestamp)
	lastAppliedTs, lastAppliedErr := strconv.Atoi(lastAppliedTimestamp)

	// Determine if this is a new command based on timestamp or content changes
	isNewCommand := false
	if (manualErr == nil && lastAppliedErr == nil && manualTs > lastAppliedTs) ||
		(manualErr != nil || lastAppliedErr != nil) && manualTimestamp > lastAppliedTimestamp {
		isNewCommand = true
		c.recordEventInfof(canary, "New manual control command received at %s", manualState.Timestamp)
	}

	hasUpdateWeight := false

	if isNewCommand {
		// apply new weight if specified
		if manualState.Weight != nil {
			weight := *manualState.Weight
			if weight < 0 || weight > 100 {
				return false, fmt.Errorf("invalid manual weight %d, must be between 0 and 100", weight)
			}

			// only set routes if weight is different
			if canary.Status.CanaryWeight != weight {
				if err := meshRouter.SetRoutes(canary, 100-weight, weight, false); err != nil {
					return false, fmt.Errorf("failed to set manual traffic weight: %w", err)
				}
				c.recorder.SetWeight(canary, 100-weight, weight)
				canary.Status.CanaryWeight = weight
				c.recordEventInfof(canary, "Manual weight set to %d%%", weight)
				hasUpdateWeight = true
			}
		}

		// update status to reflect the new manual state has been applied
		canary.Status.LastAppliedManualTimestamp = manualState.Timestamp
		canary.Status.ManualState = manualState // Make sure we update the entire manual state
		if canary.Status.ManualState.Paused {
			canary.Status.Phase = flaggerv1.CanaryPhaseWaiting
		} else {
			// If not paused, make sure we're not in waiting phase
			if canary.Status.Phase == flaggerv1.CanaryPhaseWaiting {
				canary.Status.Phase = flaggerv1.CanaryPhaseProgressing
			}
		}

		if err := canaryController.SyncStatus(canary, canary.Status); err != nil {
			return false, fmt.Errorf("failed to sync status for manual control: %w", err)
		}

		return hasUpdateWeight || canary.Status.ManualState.Paused, nil
	}

	return canary.Status.ManualState.Paused, nil
}

func (c *Controller) runPromotionTrafficShift(canary *flaggerv1.Canary, canaryController canary.Controller,
	meshRouter router.Interface, provider string, canaryWeight int, primaryWeight int) {
	// finalize promotion since no traffic shifting is possible for Kubernetes CNI
	if provider == flaggerv1.KubernetesProvider {
		if err := canaryController.SetStatusPhase(canary, flaggerv1.CanaryPhaseFinalising); err != nil {
			c.recordEventWarningf(canary, "%v", err)
		}
		return
	}

	// route all traffic to primary in one go when promotion step wight is not set
	if canary.Spec.Analysis.StepWeightPromotion == 0 {
		c.recordEventInfof(canary, "Routing all traffic to primary")
		if err := meshRouter.SetRoutes(canary, c.totalWeight(canary), 0, false); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}
		c.recorder.SetWeight(canary, c.totalWeight(canary), 0)
		if err := canaryController.SetStatusPhase(canary, flaggerv1.CanaryPhaseFinalising); err != nil {
			c.recordEventWarningf(canary, "%v", err)
		} else {
			c.recordEventInfof(canary, "Promotion completed! Routing all traffic to primary. %s.%s", canary.Spec.TargetRef.Name, canary.Namespace)
		}
		return
	}

	// increment the primary traffic weight until it reaches total weight
	if canaryWeight > 0 {
		primaryWeight += canary.GetAnalysis().StepWeightPromotion
		if primaryWeight > c.totalWeight(canary) {
			primaryWeight = c.totalWeight(canary)
		}
		canaryWeight -= canary.GetAnalysis().StepWeightPromotion
		if canaryWeight < 0 {
			canaryWeight = 0
		}
		if err := meshRouter.SetRoutes(canary, primaryWeight, canaryWeight, false); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}
		c.recorder.SetWeight(canary, primaryWeight, canaryWeight)
		c.recordEventInfof(canary, "Advance %s.%s primary weight %v", canary.Name, canary.Namespace, primaryWeight)

		// finalize promotion
		if primaryWeight == c.totalWeight(canary) {
			if err := canaryController.SetStatusPhase(canary, flaggerv1.CanaryPhaseFinalising); err != nil {
				c.recordEventWarningf(canary, "%v", err)
			} else {
				c.recordEventInfof(canary, "Promotion completed! Routing all traffic to primary. %s.%s", canary.Spec.TargetRef.Name, canary.Namespace)
			}
		} else {
			if err := canaryController.SetStatusWeight(canary, canaryWeight); err != nil {
				c.recordEventWarningf(canary, "%v", err)
			} else {
				c.recordEventInfof(canary, "Advance %s.%s canary weight %v", canary.Name, canary.Namespace, canaryWeight)
			}
		}
	}

	return

}

func (c *Controller) runCanary(canary *flaggerv1.Canary, canaryController canary.Controller,
	meshRouter router.Interface, mirrored bool, canaryWeight int, primaryWeight int, maxWeight int) {
	primaryName := fmt.Sprintf("%s-primary", canary.Spec.TargetRef.Name)

	// For stepWeights with overflow, we should promote when canaryWeight reaches totalWeight
	shouldPromote := canaryWeight >= maxWeight
	c.logger.With("canary", fmt.Sprintf("%s.%s", canary.Name, canary.Namespace)).
		Infof("runCanary: initial shouldPromote=%t, canaryWeight=%d, maxWeight=%d", shouldPromote, canaryWeight, maxWeight)
	// Special case for stepWeights with overflow
	if len(canary.GetAnalysis().StepWeights) > 0 {
		lastStepWeight := canary.GetAnalysis().StepWeights[len(canary.GetAnalysis().StepWeights)-1]
		totalWeight := c.totalWeight(canary)
		// If the last step is beyond totalWeight, promote when we reach totalWeight
		if lastStepWeight > totalWeight && canaryWeight >= totalWeight {
			shouldPromote = true
		}
		c.logger.With("canary", fmt.Sprintf("%s.%s", canary.Name, canary.Namespace)).
			Infof("runCanary: stepWeights check lastStepWeight=%d, totalWeight=%d, shouldPromote=%t", lastStepWeight, totalWeight, shouldPromote)
	}

	// Additional check: if we've reached maximum possible weight, we should promote
	totalWeight := c.totalWeight(canary)
	if canaryWeight >= totalWeight {
		shouldPromote = true
		c.logger.With("canary", fmt.Sprintf("%s.%s", canary.Name, canary.Namespace)).
			Infof("runCanary: totalWeight check canaryWeight=%d, totalWeight=%d, shouldPromote=%t", canaryWeight, totalWeight, shouldPromote)
	}

	// increase traffic weight
	if !shouldPromote {
		// If in "mirror" mode, do one step of mirroring before shifting traffic to canary.
		// When mirroring, all requests go to primary and canary, but only responses from
		// primary go back to the user.

		var nextStepWeight int = c.nextStepWeight(canary, canaryWeight)
		if canary.GetAnalysis().Mirror && canaryWeight == 0 {
			if !mirrored {
				mirrored = true
				primaryWeight = c.totalWeight(canary)
				canaryWeight = 0
			} else {
				mirrored = false
				primaryWeight = c.totalWeight(canary) - nextStepWeight
				canaryWeight = nextStepWeight
			}
			c.logCanaryEvent(canary, fmt.Sprintf("Mirror step %d/%d/%t", primaryWeight, canaryWeight, mirrored), zapcore.InfoLevel)
		} else {

			primaryWeight -= nextStepWeight
			if primaryWeight < 0 {
				primaryWeight = 0
			}
			canaryWeight += nextStepWeight
			if canaryWeight > c.totalWeight(canary) {
				canaryWeight = c.totalWeight(canary)
			}
		}

		if err := meshRouter.SetRoutes(canary, primaryWeight, canaryWeight, mirrored); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}

		if err := canaryController.SetStatusWeight(canary, canaryWeight); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}

		c.recorder.SetWeight(canary, primaryWeight, canaryWeight)
		c.recordEventInfof(canary, "Advance %s.%s canary weight %v", canary.Name, canary.Namespace, canaryWeight)
		return
	}

	// promote canary - max weight reached
	if shouldPromote {
		c.logger.With("canary", fmt.Sprintf("%s.%s", canary.Name, canary.Namespace)).
			Infof("runCanary: promoting canary, shouldPromote=%t, canaryWeight=%d, maxWeight=%d", shouldPromote, canaryWeight, maxWeight)
		// check promotion gate
		promote := c.runConfirmPromotionHooks(canary, canaryController)
		c.logger.With("canary", fmt.Sprintf("%s.%s", canary.Name, canary.Namespace)).
			Infof("runCanary: promotion gate result: %t", promote)
		if !promote {
			c.logger.With("canary", fmt.Sprintf("%s.%s", canary.Name, canary.Namespace)).
				Infof("runCanary: promotion gate not passed")
			return
		}

		// update primary spec
		c.recordEventInfof(canary, "Copying %s.%s template spec to %s.%s",
			canary.Spec.TargetRef.Name, canary.Namespace, primaryName, canary.Namespace)
		if err := canaryController.Promote(canary); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}

		// update status phase
		if err := canaryController.SetStatusPhase(canary, flaggerv1.CanaryPhasePromoting); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		} else {
			c.recordEventInfof(canary, "Promoting %s.%s to primary", canary.Spec.TargetRef.Name, canary.Namespace)
		}
	}
}

func (c *Controller) runAB(canary *flaggerv1.Canary, canaryController canary.Controller,
	meshRouter router.Interface) {
	primaryName := fmt.Sprintf("%s-primary", canary.Spec.TargetRef.Name)

	// route traffic to canary and increment iterations
	if canary.GetAnalysis().Iterations > canary.Status.Iterations {
		if err := meshRouter.SetRoutes(canary, 0, c.totalWeight(canary), false); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}
		c.recorder.SetWeight(canary, 0, c.totalWeight(canary))

		if err := canaryController.SetStatusIterations(canary, canary.Status.Iterations+1); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}
		c.recordEventInfof(canary, "Advance %s.%s canary iteration %v/%v",
			canary.Name, canary.Namespace, canary.Status.Iterations+1, canary.GetAnalysis().Iterations)
		return
	}

	// check promotion gate
	if promote := c.runConfirmPromotionHooks(canary, canaryController); !promote {
		return
	}

	// promote canary - max iterations reached
	if canary.GetAnalysis().Iterations == canary.Status.Iterations {
		c.recordEventInfof(canary, "Copying %s.%s template spec to %s.%s",
			canary.Spec.TargetRef.Name, canary.Namespace, primaryName, canary.Namespace)
		if err := canaryController.Promote(canary); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}

		// update status phase
		if err := canaryController.SetStatusPhase(canary, flaggerv1.CanaryPhasePromoting); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		} else {
			c.recordEventInfof(canary, "Promoting %s.%s to primary", canary.Spec.TargetRef.Name, canary.Namespace)
		}
	}
}

func (c *Controller) runBlueGreen(canary *flaggerv1.Canary, canaryController canary.Controller,
	meshRouter router.Interface, provider string, mirrored bool) {
	primaryName := fmt.Sprintf("%s-primary", canary.Spec.TargetRef.Name)

	// increment iterations
	if canary.GetAnalysis().Iterations > canary.Status.Iterations {
		// If in "mirror" mode, mirror requests during the entire B/G canary test
		if provider != "kubernetes" &&
			canary.GetAnalysis().Mirror && !mirrored {
			if err := meshRouter.SetRoutes(canary, c.totalWeight(canary), 0, true); err != nil {
				c.recordEventWarningf(canary, "%v", err)
			}
			c.logCanaryEvent(canary, "Start traffic mirroring", zapcore.InfoLevel)
		}
		if err := canaryController.SetStatusIterations(canary, canary.Status.Iterations+1); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}
		c.recordEventInfof(canary, "Advance %s.%s canary iteration %v/%v",
			canary.Name, canary.Namespace, canary.Status.Iterations+1, canary.GetAnalysis().Iterations)
		return
	}

	// check promotion gate
	if promote := c.runConfirmPromotionHooks(canary, canaryController); !promote {
		return
	}

	// route all traffic to canary - max iterations reached
	if canary.GetAnalysis().Iterations == canary.Status.Iterations {
		if provider != "kubernetes" {
			if canary.GetAnalysis().Mirror {
				c.recordEventInfof(canary, "Stop traffic mirroring and route all traffic to canary")
			} else {
				c.recordEventInfof(canary, "Routing all traffic to canary")
			}
			if err := meshRouter.SetRoutes(canary, 0, c.totalWeight(canary), false); err != nil {
				c.recordEventWarningf(canary, "%v", err)
				return
			}
			c.recorder.SetWeight(canary, 0, c.totalWeight(canary))
		}

		// increment iterations
		if err := canaryController.SetStatusIterations(canary, canary.Status.Iterations+1); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}
		c.recordEventInfof(canary, "Advance %s.%s canary iteration %v/%v",
			canary.Name, canary.Namespace, canary.Status.Iterations+1, canary.GetAnalysis().Iterations)
		return
	}

	// promote canary - max iterations reached
	if canary.GetAnalysis().Iterations < canary.Status.Iterations {
		c.recordEventInfof(canary, "Copying %s.%s template spec to %s.%s",
			canary.Spec.TargetRef.Name, canary.Namespace, primaryName, canary.Namespace)
		if err := canaryController.Promote(canary); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}

		// update status phase
		if err := canaryController.SetStatusPhase(canary, flaggerv1.CanaryPhasePromoting); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		} else {
			c.recordEventInfof(canary, "Promoting %s.%s to primary", canary.Spec.TargetRef.Name, canary.Namespace)
		}
	}

}

func (c *Controller) runAnalysis(canary *flaggerv1.Canary) (bool, error) {
	// run external checks
	for _, webhook := range canary.GetAnalysis().Webhooks {
		if webhook.Type == "" || webhook.Type == flaggerv1.RolloutHook {
			_, err := CallWebhookWithResponse(*canary, flaggerv1.CanaryPhaseProgressing, webhook)
			if err != nil {
				c.recordEventWarningf(canary, "Halt %s.%s advancement external check %s failed %v",
					canary.Name, canary.Namespace, webhook.Name, err)
				return false, err
			}
		}
	}

	ok, err := c.runMetricChecks(canary)
	if !ok {
		return ok, err
	}

	return true, nil
}

func (c *Controller) shouldSkipAnalysis(canary *flaggerv1.Canary, canaryController canary.Controller, meshRouter router.Interface, scalerReconciler canary.ScalerReconciler, err error, retriable bool) bool {
	skipAnalysis := canary.SkipAnalysis()
	skipCanary := false

	if skipCanary = c.runSkipHooks(canary, canary.Status.Phase); skipCanary {
		c.recordEventWarningf(canary, "Skip Canary %s.%s manual webhook invoked", canary.Name, canary.Namespace)
		c.alert(canary, fmt.Sprintf("Skip Canary %s.%s manual webhook invoked", canary.Name, canary.Namespace), false, flaggerv1.SeverityWarn)
	}
	if !skipAnalysis && !skipCanary {
		return false
	}

	// regardless if analysis is being skipped, rollback if canary failed to progress
	if !retriable {
		c.recordEventWarningf(canary, "Rolling back %s.%s progress deadline exceeded %v", canary.Name, canary.Namespace, err)
		c.alert(canary, fmt.Sprintf("Rolling back %s.%s progress deadline exceeded %v", canary.Name, canary.Namespace, err), false, flaggerv1.SeverityError)
		c.rollback(canary, canaryController, meshRouter, scalerReconciler)

		return true
	}

	c.recordEventWarningf(canary, "Skipping analysis for %s.%s", canary.Name, canary.Namespace)

	// route all traffic to primary
	primaryWeight := c.totalWeight(canary)
	canaryWeight := 0
	if err := meshRouter.SetRoutes(canary, primaryWeight, canaryWeight, false); err != nil {
		c.recordEventWarningf(canary, "%v", err)
		return true
	}
	c.recorder.SetWeight(canary, primaryWeight, canaryWeight)

	// copy spec and configs from canary to primary
	c.recordEventInfof(canary, "Copying %s.%s template spec to %s-primary.%s",
		canary.Spec.TargetRef.Name, canary.Namespace, canary.Spec.TargetRef.Name, canary.Namespace)
	if err := canaryController.Promote(canary); err != nil {
		c.recordEventWarningf(canary, "%v", err)
		return true
	}

	if scalerReconciler != nil {
		if err := scalerReconciler.ReconcilePrimaryScaler(canary, false); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return true
		}
		if err := scalerReconciler.PauseTargetScaler(canary); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return true
		}
	}

	// shutdown canary
	if err := canaryController.ScaleToZero(canary); err != nil {
		c.recordEventWarningf(canary, "%v", err)
		return true
	}

	// update status phase
	if err := canaryController.SetStatusPhase(canary, flaggerv1.CanaryPhaseSucceeded); err != nil {
		c.recordEventWarningf(canary, "%v", err)
		return true
	}

	// notify
	c.recorder.SetStatus(canary, flaggerv1.CanaryPhaseSucceeded)
	c.recordEventInfof(canary, "Promotion completed! Scaling down %s.%s", canary.Spec.TargetRef.Name, canary.Namespace)
	c.alert(canary, "Canary analysis was skipped, promotion finished.",
		false, flaggerv1.SeveritySuccess)

	return true
}

func (c *Controller) shouldAdvance(canary *flaggerv1.Canary, canaryController canary.Controller) (bool, error) {
	if canary.Status.Phase == flaggerv1.CanaryPhaseProgressing ||
		canary.Status.Phase == flaggerv1.CanaryPhaseWaiting ||
		canary.Status.Phase == flaggerv1.CanaryPhaseWaitingPromotion ||
		canary.Status.Phase == flaggerv1.CanaryPhasePromoting ||
		canary.Status.Phase == flaggerv1.CanaryPhaseFinalising {
		return true, nil
	}

	// Make sure to sync lastAppliedSpec even if the canary is in a failed state.
	if canary.Status.Phase == flaggerv1.CanaryPhaseFailed {
		if err := canaryController.SyncStatus(canary, canary.Status); err != nil {
			c.recordEventWarningf(canary, "Failed to sync canary status: %v", err)
			return false, err
		}
	}

	newTarget, err := canaryController.HasTargetChanged(canary)
	if err != nil {
		return false, err
	}
	if newTarget {
		return newTarget, nil
	}

	newCfg, err := canaryController.HaveDependenciesChanged(canary)
	if err != nil {
		return false, err
	}

	return newCfg, nil

}

func (c *Controller) checkCanaryStatus(canary *flaggerv1.Canary, canaryController canary.Controller, scalerReconciler canary.ScalerReconciler, shouldAdvance bool) bool {
	c.recorder.SetStatus(canary, canary.Status.Phase)
	if canary.Status.Phase == flaggerv1.CanaryPhaseProgressing ||
		canary.Status.Phase == flaggerv1.CanaryPhaseWaitingPromotion ||
		canary.Status.Phase == flaggerv1.CanaryPhasePromoting ||
		canary.Status.Phase == flaggerv1.CanaryPhaseFinalising {
		return true
	}

	// If canary is waiting due to manual traffic control, don't advance the status
	if canary.Status.Phase == flaggerv1.CanaryPhaseWaiting &&
		canary.Status.ManualState != nil &&
		canary.Status.ManualState.Paused {
		return true
	}

	var err error
	canary, err = c.flaggerClient.FlaggerV1beta1().Canaries(canary.Namespace).Get(context.TODO(), canary.Name, metav1.GetOptions{})
	if err != nil {
		c.logCanaryEvent(canary, "Failed get canary", zapcore.ErrorLevel)
		return false
	}

	if shouldAdvance {
		// check confirm-rollout gate
		if isApproved := c.runConfirmRolloutHooks(canary, canaryController); !isApproved {
			return false
		}
		c.recorder.Control.StartOperation(canary, "start")

		canaryPhaseProgressing := canary.DeepCopy()
		canaryPhaseProgressing.Status.Phase = flaggerv1.CanaryPhaseProgressing

		c.logCanaryEvent(canaryPhaseProgressing, fmt.Sprintf("New revision detected! Scaling up %s.%s", canaryPhaseProgressing.Spec.TargetRef.Name, canaryPhaseProgressing.Namespace), zapcore.InfoLevel)
		if scalerReconciler != nil {
			err = scalerReconciler.ResumeTargetScaler(canary)
			if err != nil {
				c.recordEventWarningf(canary, "%v", err)
				return false
			}
		}
		if err := canaryController.ScaleFromZero(canary); err != nil {
			c.recordEventErrorf(canary, "%v", err)
			return false
		}
		if err := canaryController.SyncStatus(canary, flaggerv1.CanaryStatus{Phase: flaggerv1.CanaryPhaseProgressing, LastStartTime: metav1.Now()}); err != nil {
			c.logCanaryEvent(canary, "Failed to update canary status", zapcore.ErrorLevel)
			return false
		}
		c.recorder.SetStatus(canary, flaggerv1.CanaryPhaseProgressing)
		c.recorder.Control.CompleteOperation(canary, "start")
		canary, err = c.flaggerClient.FlaggerV1beta1().Canaries(canary.Namespace).Get(context.TODO(), canary.Name, metav1.GetOptions{})
		if err != nil {
			c.logCanaryEvent(canary, "Failed get canary", zapcore.ErrorLevel)
			return false
		}
		c.recordEventInfof(canaryPhaseProgressing, "New revision detected! Scaling up %s.%s", canaryPhaseProgressing.Spec.TargetRef.Name, canaryPhaseProgressing.Namespace)
		// send alert
		c.alert(canary, fmt.Sprintf("New revision detected, progressing canary analysis! Scaling up %s.%s", canaryPhaseProgressing.Spec.TargetRef.Name, canaryPhaseProgressing.Namespace),
			true, flaggerv1.SeverityInfo)
		return false
	}
	return false
}

func (c *Controller) hasCanaryRevisionChanged(canary *flaggerv1.Canary, canaryController canary.Controller) bool {
	if canary.Status.Phase == flaggerv1.CanaryPhaseProgressing ||
		canary.Status.Phase == flaggerv1.CanaryPhaseWaitingPromotion {
		if diff, _ := canaryController.HasTargetChanged(canary); diff {
			return true
		}
		if diff, _ := canaryController.HaveDependenciesChanged(canary); diff {
			return true
		}
	}
	return false
}

func (c *Controller) rollback(canary *flaggerv1.Canary, canaryController canary.Controller,
	meshRouter router.Interface, scalerReconciler canary.ScalerReconciler) {
	c.recorder.Control.StartOperation(canary, "rollback")
	if canary.Status.FailedChecks >= canary.GetAnalysisThreshold() {
		c.recordEventWarningf(canary, "Rolling back %s.%s failed checks threshold reached %v",
			canary.Name, canary.Namespace, canary.Status.FailedChecks)
		c.alert(canary, fmt.Sprintf("Rolling back %s.%s .Failed checks threshold reached %v", canary.Name, canary.Namespace, canary.Status.FailedChecks),
			false, flaggerv1.SeverityError)
	}

	// route all traffic back to primary
	primaryWeight := c.totalWeight(canary)
	canaryWeight := 0
	if err := meshRouter.SetRoutes(canary, primaryWeight, canaryWeight, false); err != nil {
		c.recordEventWarningf(canary, "%v", err)
		return
	}

	canaryPhaseFailed := canary.DeepCopy()
	canaryPhaseFailed.Status.Phase = flaggerv1.CanaryPhaseFailed
	c.recordEventWarningf(canaryPhaseFailed, "Canary failed! Scaling down %s.%s",
		canaryPhaseFailed.Name, canaryPhaseFailed.Namespace)

	c.recorder.SetWeight(canary, primaryWeight, canaryWeight)

	if scalerReconciler != nil {
		if err := scalerReconciler.PauseTargetScaler(canary); err != nil {
			c.recordEventWarningf(canary, "%v", err)
			return
		}
	}
	// shutdown canary
	if err := canaryController.ScaleToZero(canary); err != nil {
		c.recordEventWarningf(canary, "%v", err)
		return
	}

	// mark canary as failed
	canaryStatus := flaggerv1.CanaryStatus{
		Phase:                      flaggerv1.CanaryPhaseFailed,
		CanaryWeight:               0,
		LastAppliedManualTimestamp: fmt.Sprintf("%d", time.Now().Unix()),
	}
	if canaryPhaseFailed.Status.ManualState != nil {
		canaryStatus.ManualState = &flaggerv1.CanaryManualState{}
	}
	if err := canaryController.SyncStatus(canary, canaryStatus); err != nil {
		c.logCanaryEvent(canary, fmt.Sprintf("Canary Failed. Scaled down %s.%s", canary.Spec.TargetRef.Name, canary.Namespace), zapcore.ErrorLevel)
		return
	} else {
		c.recordEventInfof(canary, "Canary Failed. Scaled down %s.%s", canary.Spec.TargetRef.Name, canary.Namespace)
	}

	c.recorder.SetStatus(canary, flaggerv1.CanaryPhaseFailed)
	c.recorder.Control.CompleteOperation(canary, "rollback")
	c.runPostRolloutHooks(canary, flaggerv1.CanaryPhaseFailed)
}

func (c *Controller) setPhaseInitialized(cd *flaggerv1.Canary, canaryController canary.Controller) error {
	if cd.Status.Phase == "" || cd.Status.Phase == flaggerv1.CanaryPhaseInitializing {
		cd.Status.Phase = flaggerv1.CanaryPhaseInitialized
		if err := canaryController.SyncStatus(cd, flaggerv1.CanaryStatus{Phase: flaggerv1.CanaryPhaseInitialized}); err != nil {
			return fmt.Errorf("failed to sync canary %s.%s status: %w", cd.Name, cd.Namespace, err)
		}

		canary, err := c.flaggerClient.FlaggerV1beta1().Canaries(cd.Namespace).Get(context.TODO(), cd.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get canary %s.%s: %w", cd.Name, cd.Namespace, err)
		}
		// We need to sync the LastAppliedSpec and TrackedConfigs of the `cd` Canary object as it
		// is used later to determine whether target revision has changed in `shouldAdvance()`.
		cd.Status.LastAppliedSpec = canary.Status.LastAppliedSpec
		cd.Status.TrackedConfigs = canary.Status.TrackedConfigs

		c.recorder.SetStatus(cd, flaggerv1.CanaryPhaseInitialized)
		c.recordEventInfof(cd, "Initialization done! %s.%s", cd.Name, cd.Namespace)
		c.alert(cd, fmt.Sprintf("New %s detected, initialization completed! %s.%s", cd.Spec.TargetRef.Kind, cd.Name, cd.Namespace),
			true, flaggerv1.SeveritySuccess)
	}
	return nil
}

func (c *Controller) setPhaseInitializing(cd *flaggerv1.Canary) error {
	phase := flaggerv1.CanaryPhaseInitializing
	firstTry := true
	name, ns := cd.GetName(), cd.GetNamespace()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() (err error) {
		if !firstTry {
			cd, err = c.flaggerClient.FlaggerV1beta1().Canaries(ns).Get(context.TODO(), name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("canary %s.%s get query failed: %w", name, ns, err)
			}
		}

		if ok, conditions := canary.MakeStatusConditions(cd, phase); ok {
			cdCopy := cd.DeepCopy()
			cdCopy.Status.Conditions = conditions
			cdCopy.Status.LastTransitionTime = metav1.Now()
			cdCopy.Status.Phase = phase
			_, err = c.flaggerClient.FlaggerV1beta1().Canaries(cd.Namespace).UpdateStatus(context.TODO(), cdCopy, metav1.UpdateOptions{})
		}
		firstTry = false
		return
	})

	if err != nil {
		return fmt.Errorf("failed after retries: %w", err)
	}
	return nil
}

func (c *Controller) recorderMetrics(cd *flaggerv1.Canary) {
	name := cd.GetName()
	namespace := cd.GetNamespace()

	// check if the canary exists
	cd, err := c.flaggerClient.FlaggerV1beta1().Canaries(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		c.logCanaryEvent(cd, fmt.Sprintf("Canary %s.%s not found", name, namespace), zapcore.ErrorLevel)
		return
	}

	c.recorder.SetStatus(cd, cd.Status.Phase)

	// override the global provider if one is specified in the canary spec
	provider := c.meshProvider
	if cd.Spec.Provider != "" {
		provider = cd.Spec.Provider
	}

	// init controller based on target kind
	canaryController := c.canaryFactory.Controller(cd.Spec.TargetRef.Kind)

	labelSelector, _, _, _, err := canaryController.GetMetadata(cd)
	if err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}
	// init mesh router
	meshRouter := c.routerFactory.MeshRouter(provider, labelSelector)

	// get the routing settings
	primaryWeight, canaryWeight, _, err := meshRouter.GetRoutes(cd)
	if err != nil {
		c.recordEventWarningf(cd, "%v", err)
		return
	}

	c.recorder.SetWeight(cd, primaryWeight, canaryWeight)
}
