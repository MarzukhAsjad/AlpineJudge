package pkg

import (
	"context"
	"dispatcher/internal"
	"log/slog"

	// "dispatcher/internal"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"shared"
	"syscall"
	"time"
)

func Dispatcher() {
	/*
		Signal aware context
		Auto cancels when the OS sends termination commands like Ctrl+C (SIGINT) or systemd stop (SIGTERM).
	*/
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. initialize infrastructures
	slog.Info("Initiating S3 storage...")
	bucket := os.Getenv("MINIO_S3_BUCKET")
	region := os.Getenv("MINIO_S3_REGION_NAME")
	accessKey := os.Getenv("MINIO_S3_USERNAME")
	secretKey := os.Getenv("MINIO_S3_PASSWORD")
	s3Endpoint := os.Getenv("MINIO_S3_API")

	s3m, err := shared.InitS3Manager(ctx, bucket, region, accessKey, secretKey, s3Endpoint)
	if err != nil {
		slog.Error("Failed to spin up S3", "error", err)
		os.Exit(1)
	}

	slog.Info("Initiating RMQ connection...")
	amqpURL := os.Getenv("RABBITMQ_URL")
	if amqpURL == "" {
		slog.Error("RMQ url not found in environment!")
		os.Exit(1)
	}

	slog.Info("S3 config initialized", "bucket", bucket, "region", region, "accessKey", accessKey, "secretKey", secretKey, "s3Endpoint", s3Endpoint)

	rmqMgr, err := shared.NewRMQManager(ctx, amqpURL)
	if err != nil {
		slog.Error("Failed to spin up RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("Closing RabbitMQ sockets...")
		rmqMgr.Close()
	}()

	slog.Info("Starting Dispatcher HTTP server...")
	server := internal.InitHTTPServer(ctx, s3m, rmqMgr)

	// 2. background HTTP server listener to make it non-blocking
	go func() {
		slog.Info("Dispatcher listening securely on", "address", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Critical HTTP server crash", "error", err)
			os.Exit(1)
		}
	}()

	// wait for OS signal to stop
	<-ctx.Done()
	slog.Info("Termination signal caught! Initiating graceful teardown protocol.",  "shutdown_reason", ctx.Err())

	// 3. raceful shutdown Phase
	// Force-kill the HTTP engine if it takes longer than 5 seconds to clear out pending traffic
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown warning: forced termination executed", "error", err)
	} else {
		slog.Info("HTTP server closed cleanly")
	}

	slog.Info("Dispatcher daemon terminated cleanly")
}
