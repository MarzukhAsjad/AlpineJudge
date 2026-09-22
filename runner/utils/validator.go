package utils

import (
	"context"
	"encoding/json"
	"log/slog"
	"shared"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ProcessJobSpec(
	ctx context.Context, msg amqp.Delivery, ssequeue string) (shared.JobSpec, error) {

	slog.Debug("Processing job spec", "len", len(msg.Body))

	var jobspec shared.JobSpec
	err := json.Unmarshal(msg.Body, &jobspec)

	// NACK bad JSON and move on
	if err != nil {
		slog.Error("Error processing job spec in JSON", "error", err, "raw", string(msg.Body))
		_ = msg.Nack(false, false)
		return shared.JobSpec{}, err
	}

	// log.Printf("Processed job spec: %v\n", jobspec)
	return jobspec, nil
}
