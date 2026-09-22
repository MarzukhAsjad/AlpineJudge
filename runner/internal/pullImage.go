// prepares command to execute and runs the container
package internal

import (
	"context"
	"fmt"
	"log/slog"

	containerd "github.com/containerd/containerd"
	"github.com/containerd/containerd/errdefs"
)

// check image cache first then download image if cache miss
func getContainerImage(imageName string, client *containerd.Client, ctx context.Context) (containerd.Image, error) {

	image, err := client.GetImage(ctx, imageName)

	if err == nil {
		return image, nil
	}

	if errdefs.IsNotFound(err) {
		slog.Info("Image not found locally, pulling", "image", imageName)
		// download image
		pulledImage, err := client.Pull(ctx, imageName, containerd.WithPullUnpack)
		if err != nil {
			return nil, fmt.Errorf("failed to pull image %s: %w", imageName, err)
		}
		slog.Info("Successfully downloaded and pulled image", "image", pulledImage.Name())
		return pulledImage, nil
	}

	slog.Warn("Unexpected error occured querying image", "image", imageName, "error", err)
	return nil, err
}
