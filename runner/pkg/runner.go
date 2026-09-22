package pkg

import (
	"context"
	"log/slog"
	"os"
	"shared"
	"utils"

	"github.com/containerd/containerd"
)

/*
The top-most level of the Runner subsystem. Gets called by main()
It runs as daemon process so it's never intended to exit unless crashes internally or Linux kernel intervense
*/
func Runner() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// godotenv.Load(".env")

	rmqm, err := shared.NewRMQManager(ctx, os.Getenv("RABBITMQ_URL"))
	if err != nil {
		slog.Error("Fatal: RabbitMQ connection broken", "error", err)
		os.Exit(1)
	}
	defer rmqm.Close()

	slog.Info("Initializing S3...")
	bukcetName := os.Getenv("MINIO_S3_BUCKET")
	s3m, err := shared.InitS3Manager(
		ctx,
		bukcetName,
		os.Getenv("MINIO_S3_REGION_NAME"),
		os.Getenv("MINIO_S3_USERNAME"),
		os.Getenv("MINIO_S3_PASSWORD"),
		os.Getenv("MINIO_S3_API"),
	)
	if err != nil {
		slog.Error("Fatal: S3 Storage initialization aborted", "error", err)
		os.Exit(1)
	}
	if _, err := s3m.CreateABucket(ctx, bukcetName); err != nil {
		slog.Error("Failed to create bucket", "bucket", bukcetName, "error", err)
		os.Exit(1)
	}
	slog.Info("Initialized S3 with bucket", "bucket", bukcetName)

	slog.Info("Initializing containerd client")

	client, err := containerd.New("/run/containerd/containerd.sock")
	if err != nil {
		slog.Error("Fatal: containerd client initialization aborted", "error", err)
		os.Exit(1)
	}
	slog.Info("Initialized containerd client")
	defer client.Close()

	edps := utils.EngineDeps{
		Client:    client,
		S3:        s3m,
		Rmq:       rmqm,
		JobQueue:  os.Getenv("RABBITMQ_QUEUE_NAME"),
		SSEQueue:  os.Getenv("RABBITMQ_SSE_QUEUE_NAME"),
		Namespace: os.Getenv("CONTAINER_NAMESPACE"),
	}

	InitRunner(ctx, edps)
}
