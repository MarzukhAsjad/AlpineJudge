package main

import (
	ajagent "ajagent/pkg"
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: true,
		}),
	))
	ajagent.RunnerAgent()
}
