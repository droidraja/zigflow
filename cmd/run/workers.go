/*
 * Copyright 2025 - 2026 Zigflow authors <https://github.com/zigflow/zigflow/graphs/contributors>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package run

import (
	"context"
	"fmt"
	"sort"

	gh "github.com/mrsimonemms/golang-helpers"
	"github.com/mrsimonemms/golang-helpers/temporal"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zigflow/zigflow/pkg/codec"
	"github.com/zigflow/zigflow/pkg/zigflow"
	"github.com/zigflow/zigflow/pkg/zigflow/activities"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/sysinfo"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// newTemporalConnection is the function used to establish a Temporal client. It
// is a package-level variable so tests can substitute a test double without
// spinning up a real Temporal server.
var newTemporalConnection = temporal.NewConnection

// newWorker is the function used to create a Temporal worker. It is a
// package-level variable so tests can substitute a test double to capture
// worker.Options (including DeploymentOptions) without spinning up a real
// Temporal server.
var newWorker = worker.New

// newWorkflow registers a workflow definition on a Temporal worker. It is a
// package-level variable so tests can substitute a test double that captures
// the arguments passed in, without registering a real workflow.
var newWorkflow = zigflow.NewWorkflow

// registerSharedActivity and registerDynamicWorkflow are narrow registration
// seams used by command tests to record worker lifecycle without starting a
// Temporal server.
var registerSharedActivity = func(w worker.Worker, activity any) {
	w.RegisterActivity(activity)
}

var registerDynamicWorkflow = func(
	w worker.Worker,
	handler any,
	opts workflow.DynamicRegisterOptions,
) {
	w.RegisterDynamicWorkflow(handler, opts)
}

// runScheduleUpdates updates Temporal schedules for all workflow registrations
// before any worker is started.
func runScheduleUpdates(
	ctx context.Context,
	temporalClient client.Client,
	registrations []*workflowRegistration,
	envvars map[string]any,
) error {
	for _, reg := range registrations {
		log.Info().Str("workflow", reg.WorkflowType).Msg("Updating schedules")
		if err := zigflow.UpdateSchedules(ctx, temporalClient, reg.Definition, envvars); err != nil {
			return gh.FatalError{
				Cause: err,
				WithParams: func(l *zerolog.Event) *zerolog.Event {
					return l.
						Str("workflow", reg.WorkflowType).
						Str("taskQueue", reg.TaskQueue).
						Str("file", reg.SourceFile)
				},
				Msg: "Error updating Temporal schedules",
			}
		}
	}
	return nil
}

// startAllWorkers starts each worker in the map and returns the list of
// successfully started workers. Workers are started in sorted task-queue order
// so that startup sequence is deterministic regardless of map iteration order.
// On failure it stops any workers that were already started before returning
// the error, so the caller does not need to track partial state.
func startAllWorkers(workers map[string]worker.Worker) ([]worker.Worker, error) {
	taskQueues := make([]string, 0, len(workers))
	for tq := range workers {
		taskQueues = append(taskQueues, tq)
	}
	sort.Strings(taskQueues)

	started := make([]worker.Worker, 0, len(workers))
	for _, taskQueue := range taskQueues {
		w := workers[taskQueue]
		log.Info().Str("task-queue", taskQueue).Msg("Starting worker")
		if err := w.Start(); err != nil {
			for _, sw := range started {
				sw.Stop()
			}
			return nil, gh.FatalError{
				Cause: err,
				WithParams: func(l *zerolog.Event) *zerolog.Event {
					return l.Str("task-queue", taskQueue)
				},
				Msg: "Unable to start worker",
			}
		}
		started = append(started, w)
	}
	return started, nil
}

// buildWorkersByTaskQueue creates one Temporal worker per distinct task queue
// and registers all workflow definitions onto the appropriate worker. Workflows
// that share a task queue are registered on the same worker.
func buildWorkersByTaskQueue(
	temporalClient client.Client,
	registrations []*workflowRegistration,
	envvars map[string]any,
	opts *runOptions,
) (map[string]worker.Worker, error) {
	if opts.MaxConcurrentWorkflowTaskExecutionSize == 1 {
		return nil, gh.FatalError{
			Msg: "Max concurrent workflow task execution size cannot be set to 1",
		}
	}

	dynamicQueues, err := dynamicTaskQueues(opts)
	if err != nil {
		return nil, err
	}

	workerOpts, err := temporalWorkerOptions(opts)
	if err != nil {
		return nil, err
	}

	queueSet := make(map[string]struct{}, len(registrations)+len(dynamicQueues))
	for _, reg := range registrations {
		queueSet[reg.TaskQueue] = struct{}{}
	}
	for _, taskQueue := range dynamicQueues {
		queueSet[taskQueue] = struct{}{}
	}

	queueNames := make([]string, 0, len(queueSet))
	for taskQueue := range queueSet {
		queueNames = append(queueNames, taskQueue)
	}
	sort.Strings(queueNames)

	workers := make(map[string]worker.Worker, len(queueNames))
	for _, taskQueue := range queueNames {
		w := newWorker(temporalClient, taskQueue, workerOpts)
		workers[taskQueue] = w
		log.Debug().Str("task-queue", taskQueue).Msg("Created worker for task queue")

		taskActivities := tasks.ActivitiesList()
		log.Debug().
			Str("task-queue", taskQueue).
			Int("count", len(taskActivities)).
			Msg("Registering shared activities on worker")
		for _, activity := range taskActivities {
			registerSharedActivity(w, activity)
		}
	}

	taskOpts := buildTaskOptions(opts)
	for _, reg := range registrations {
		w := workers[reg.TaskQueue]

		log.Info().
			Str("task-queue", reg.TaskQueue).
			Str("workflow", reg.WorkflowType).
			Str("file", reg.SourceFile).
			Msg("Registering workflow")

		if err := newWorkflow(w, reg.Definition, envvars, reg.Events, opts.Telemetry, taskOpts); err != nil {
			return nil, gh.FatalError{
				Cause: err,
				WithParams: func(l *zerolog.Event) *zerolog.Event {
					return l.
						Str("workflow", reg.WorkflowType).
						Str("file", reg.SourceFile)
				},
				Msg: "Unable to build workflow from DSL",
			}
		}
	}

	dynamicRegisterOpts := dynamicWorkflowRegisterOptions(opts)
	for _, taskQueue := range dynamicQueues {
		log.Info().Str("task-queue", taskQueue).Msg("Registering dynamic workflow fallback")
		registerDynamicWorkflow(
			workers[taskQueue],
			zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{
				Envvars:  envvars,
				TaskOpts: taskOpts,
			}),
			dynamicRegisterOpts,
		)
	}

	return workers, nil
}

func buildTaskOptions(opts *runOptions) *tasks.TaskOpts {
	return &tasks.TaskOpts{
		Run: &tasks.RunTaskOpts{
			Namespace:      opts.ContainerRuntimeNamespace,
			Runtime:        activities.ContainerRuntime(opts.ContainerRuntime),
			ServiceAccount: opts.ContainerRuntimeServiceAccount,
		},
	}
}

func temporalWorkerOptions(opts *runOptions) (worker.Options, error) {
	pollerAutoscaler := worker.NewPollerBehaviorAutoscaling(worker.PollerBehaviorAutoscalingOptions{})

	var deploymentOptions worker.DeploymentOptions
	if opts.EnableVersioning {
		if opts.DeploymentBuildID == "" {
			return worker.Options{}, fmt.Errorf("temporal-worker-build-id required when versioning enabled")
		}
		if opts.DeploymentName == "" {
			return worker.Options{}, fmt.Errorf("temporal-deployment-name required when versioning enabled")
		}

		log.Debug().
			Str("buildId", opts.DeploymentBuildID).
			Str("deploymentName", opts.DeploymentName).
			Str("defaultVersioningBehaviour", opts.DefaultVersioningBehaviour).
			Msg("Versioning enabled")

		deploymentOptions = worker.DeploymentOptions{
			UseVersioning:             true,
			DefaultVersioningBehavior: opts.defaultVersioningBehaviour,
			Version: worker.WorkerDeploymentVersion{
				BuildID:        opts.DeploymentBuildID,
				DeploymentName: opts.DeploymentName,
			},
		}
	}

	return worker.Options{
		WorkflowTaskPollerBehavior:             pollerAutoscaler,
		ActivityTaskPollerBehavior:             pollerAutoscaler,
		NexusTaskPollerBehavior:                pollerAutoscaler,
		WorkerStopTimeout:                      opts.GracefulShutdownTimeout,
		MaxConcurrentActivityExecutionSize:     opts.MaxConcurrentActivityExecutionSize,
		MaxConcurrentWorkflowTaskExecutionSize: opts.MaxConcurrentWorkflowTaskExecutionSize,
		TaskQueueActivitiesPerSecond:           opts.TaskQueueActivitiesPerSecond,
		DeploymentOptions:                      deploymentOptions,
		SysInfoProvider:                        sysinfo.SysInfoProvider(),
	}, nil
}

func dynamicWorkflowRegisterOptions(opts *runOptions) workflow.DynamicRegisterOptions {
	if !opts.EnableVersioning {
		return workflow.DynamicRegisterOptions{}
	}

	return workflow.DynamicRegisterOptions{
		LoadDynamicRuntimeOptions: func(
			workflow.LoadDynamicRuntimeOptionsDetails,
		) (workflow.DynamicRuntimeOptions, error) {
			return workflow.DynamicRuntimeOptions{
				VersioningBehavior: opts.defaultVersioningBehaviour,
			}, nil
		},
	}
}

// launchWorkers prepares registrations, builds workers, and starts them.
// On any error it stops any workers that were partially started before returning.
func launchWorkers(
	temporalClient client.Client,
	opts *runOptions,
	envvars map[string]any,
) ([]worker.Worker, error) {
	registrations, err := prepareRegistrations(opts)
	if err != nil {
		return nil, err
	}

	workers, err := buildWorkersByTaskQueue(temporalClient, registrations, envvars, opts)
	if err != nil {
		return nil, err
	}

	return startAllWorkers(workers)
}

// stopWorkerList stops each worker in the slice.
func stopWorkerList(workers []worker.Worker) {
	log.Info().Int("count", len(workers)).Msg("Watch: stopping workers")
	for _, w := range workers {
		w.Stop()
	}
}

// initTemporalClient creates the codec data converter and the Temporal client.
// The caller is responsible for closing the returned client.
func initTemporalClient(opts *runOptions) (client.Client, error) {
	codecType, _ := codec.ParseCodecType(opts.ConvertData)
	dataConverter, err := codec.NewDataConverter(codecType, opts.CodecEndpoint, opts.ConvertKeyPath, opts.CodecHeaders)
	if err != nil {
		return nil, err
	}

	log.Trace().Msg("Connecting to Temporal")
	tc, err := newTemporalConnection(
		temporal.WithHostPort(opts.temporal.Address),
		temporal.WithNamespace(opts.temporal.Namespace),
		temporal.WithTLS(opts.temporal.TLSEnabled, temporal.WithTLSServerName(opts.temporal.ServerName)),
		temporal.WithAuthDetection(
			opts.temporal.APIKey,
			opts.temporal.MTLSCertPath,
			opts.temporal.MTLSKeyPath,
		),
		temporal.WithDataConverter(dataConverter),
		func(o *client.Options) error {
			if opts.ConvertFailureData {
				return temporal.WithFailureConverter(dataConverter)(o)
			}
			return nil
		},
		temporal.WithZerolog(&log.Logger),
		temporal.WithPrometheusMetrics(opts.temporal.MetricsListenAddress, opts.temporal.MetricsPrefix, nil),
	)
	if err != nil {
		return nil, gh.FatalError{Cause: err, Msg: "Unable to create client"}
	}
	return tc, nil
}

// startInitialWorkers builds workers from registrations, registers the health
// check, starts telemetry, and starts all workers. It is the single call that
// takes the process from validated registrations to running workers.
func startInitialWorkers(
	ctx context.Context,
	tc client.Client,
	registrations []*workflowRegistration,
	envvars map[string]any,
	opts *runOptions,
) (map[string]worker.Worker, []worker.Worker, error) {
	workers, err := buildWorkersByTaskQueue(tc, registrations, envvars, opts)
	if err != nil {
		return nil, nil, err
	}

	taskQueues := make([]string, 0, len(workers))
	for tq := range workers {
		taskQueues = append(taskQueues, tq)
	}
	temporal.NewHealthCheck(ctx, taskQueues, opts.temporal.HealthListenAddress, tc)

	if opts.Telemetry != nil {
		opts.Telemetry.StartWorker()
	}

	started, err := startAllWorkers(workers)
	if err != nil {
		return nil, nil, err
	}
	return workers, started, nil
}

// splitWatchWorkers separates the initially started workers into the dynamic
// workers which remain stable for the lifetime of watch mode and the static
// workers which may be replaced after file changes. Registration preparation
// rejects overlapping task queues before this function is called.
func splitWatchWorkers(
	workers map[string]worker.Worker,
	dynamicTaskQueues []string,
) (dynamicWorkers, staticWorkers []worker.Worker) {
	dynamicSet := make(map[string]struct{}, len(dynamicTaskQueues))
	for _, taskQueue := range dynamicTaskQueues {
		dynamicSet[taskQueue] = struct{}{}
	}

	taskQueues := make([]string, 0, len(workers))
	for taskQueue := range workers {
		taskQueues = append(taskQueues, taskQueue)
	}
	sort.Strings(taskQueues)

	for _, taskQueue := range taskQueues {
		if _, dynamic := dynamicSet[taskQueue]; dynamic {
			dynamicWorkers = append(dynamicWorkers, workers[taskQueue])
		} else {
			staticWorkers = append(staticWorkers, workers[taskQueue])
		}
	}

	return dynamicWorkers, staticWorkers
}

// waitForShutdown blocks until the process receives an interrupt signal or the
// context is cancelled.
func waitForShutdown(ctx context.Context) {
	select {
	case <-worker.InterruptCh():
		log.Info().Msg("Received interrupt signal")
	case <-ctx.Done():
		log.Info().Msg("Context cancelled")
	}
}
