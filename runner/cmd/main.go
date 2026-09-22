package main

import (
	"local/runner/pkg"
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: true,
		}),
    ))
	pkg.Runner()
}
