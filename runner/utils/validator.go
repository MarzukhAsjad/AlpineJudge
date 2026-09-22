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
		slog.Error("Failed to unmarshal jobspec JSON", "error", err, "raw", string(msg.Body))
		_ = msg.Nack(false, false)
		return shared.JobSpec{}, err
	}

	return jobspec, nil
}
