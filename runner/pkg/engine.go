package pkg

import (
	"context"
	"local/runner/internal"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"utils"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	amqp "github.com/rabbitmq/amqp091-go"
)

func InitRunner(ctx context.Context, deps utils.EngineDeps) {

	slog.Info("Initializing Runner...")
	/*
		tmp subdirectories are created in layers.
		The base ones (/tmp/runner/sockets /tmp/runner/testsets /tmp/runner/submissions ) are created during initiation (here)
		Slot specific sockets are created during warm container creation
		Submission specific subdirectories are created during orchestration (after container getting job)
	*/

	// base subdirectories ( /tmp/runner mounted as /workspace in all containers)
	dirs := []string{
		filepath.Join("/tmp", "runner", "sockets"),
		filepath.Join("/tmp", "runner", "testsets"),
		filepath.Join("/tmp", "runner", "submissions"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0777); err != nil {
			slog.Error("Fatal: failed to create base temp directory", "dir", dir, "error", err)
			os.Exit(1)
		}
	}
	slog.Debug("Created base temp directories")

	// rmq consumer (to collect jobspecs from rmq)
	localqueue := make(chan amqp.Delivery)
	if err := deps.Rmq.Subscribe(ctx, localqueue, deps.JobQueue, "rmq-consoomer"); err != nil {
		slog.Error("Fatal: failed to initiate RMQ consumer", "error", err)
		os.Exit(1)
	}

	cCtx := namespaces.WithNamespace(ctx, deps.Namespace)

	// create warm continaers | conainerQueue is buffered with MAX_CONTAINER_CAP(15 for now) , if made unbuffered, runner will freeze shortly
	cqcap, ok := os.LookupEnv("CONTAINER_QUEUECAP")
	if !ok {
		slog.Error("Fatal: failed to retrieve CONTAINER_QUEUECAP from env")
		os.Exit(1)
	}
	maxWarmContianers, err := strconv.ParseInt(cqcap, 10, 32)
	if err != nil {
		slog.Error("Fatal: failed to convert CONTAINER_QUEUECAP to int value", "error", err)
		os.Exit(1)
	}

	containerQueue := make(chan *internal.WarmContainer, maxWarmContianers)
	countContainer := 0
	var slotCounter atomic.Uint32 // Used atomic counter to prevent concurrency issues affecting slotID uniqueness

	// warm container producer loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				countContainer++

				// create the warm container
				slotID := slotCounter.Add(1)
				slog.Debug("Creating warm container", "slot", slotID)
				warmc, err := internal.CreateWarmContainer(cCtx, deps.Client, slotID)
				if err != nil {
					slog.Warn("Warm container creation failed, retrying after cooldown", "slot", slotID, "error", err)
					time.Sleep(1 * time.Second) // short cooldown
					continue
				}
				select {
				// send the warm container in containerQueue
				case containerQueue <- warmc:
				case <-ctx.Done():
					slog.Warn("Producer context canceled, deleting container slot", "slot", slotID)
					_ = warmc.Container.Delete(cCtx, containerd.WithSnapshotCleanup)
					return
				}

			}
		}
	}()

	// main orchestration
	var wg sync.WaitGroup
	for {

		select {
		case <-ctx.Done():
			slog.Info("Shutting down runner engine")
			wg.Wait() // block until active OrchestrateSubm workers finish
			return
		case msg, ok := <-localqueue:
			if !ok {
				slog.Warn("Local queue channel closed, exiting")
				return
			}

			// worker go-routine
			wg.Add(1)
			go func(delivery amqp.Delivery) {
				defer wg.Done()

				var warmcontainer *internal.WarmContainer

				select {
				case warmcontainer = <-containerQueue:
					time.Sleep(50 * time.Millisecond) // small cooldown
				case <-time.After(3 * time.Second):

					// fallback on-demand container creation in case of queue overload
					slotID := slotCounter.Add(1)
					var err error
					warmcontainer, err = internal.CreateWarmContainer(cCtx, deps.Client, slotID)
					if err != nil {
						slog.Error("Failed to create container (on-demand)", "slot", slotID, "error", err)
					}
				}

				jobspec, err := utils.ProcessJobSpec(ctx, msg, deps.SSEQueue)
				if err != nil {
					slog.Error("Jobspec parsing error", "error", err)
					_ = delivery.Nack(false, true)
				}

				contInfo, err := OrchestrateSubm(ctx, cCtx, warmcontainer, *deps.S3, jobspec, *deps.Rmq)
				if err != nil {
					slog.Error("Orchestrator error", "error", err)
				}
				_ = delivery.Ack(false) // send ACK to rmq only after sending it downstream
				slog.Debug("Container finished", "container_info", contInfo)

			}(msg)
		}
	}
}
