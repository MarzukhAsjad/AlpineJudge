package internal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"shared"
	"strconv"
	"strings"
	"syscall"
	"time"
	"utils"

	containerd "github.com/containerd/containerd"
)

func (wc *WarmContainer) ExecSubm(
	ctx context.Context,
	rules utils.ExecRules,
	jobspec shared.JobSpec,
	rmqm shared.RMQManager,
	s3m shared.S3Manager,
) utils.ContainerInfo {

	contInfo := utils.ContainerInfo{
		SubmissionId:    jobspec.SubmissionID,
		Language:        jobspec.Language,
		Interval:        0,
		Status:          "PENDING",
		StatusInfo:      "",
		ContainerStdout: "",
		ContainerStderr: "",
	}

	_ = "submissions/" + jobspec.SubmissionID + "/"

	start := time.Now()

	// Goroutine: Accepts incoming socket connections from container & publishes to RabbitMQ in real time
	// Handle stream from this connection
	go func(c net.Conn) {
		defer c.Close()

		/*
			By default unix socket only have 64KB of log limit, which is ofter far below than a testcase might send during OLE
			Which causes overflow and unix socket connection drops silently.
			That's why a good estimation of 10 MB max log size is set with 60KB of buffer so socket connection doesn't break during OLE
		*/
		maxlogcapKB, exists := os.LookupEnv("MAX_LOG_CAP_KB")

		if !exists {
			slog.Error("Fatal: missing env var MAX_LOG_CAP_KB")
			os.Exit(1)
		}
		scanner := bufio.NewScanner(c)
		buf := make([]byte, 60*1024) // 60 KB buffer size
		maxlogcapKBn, err := strconv.ParseInt(maxlogcapKB, 10, 32)
		if err != nil {
			slog.Error("Fatal: failed converting MAX_LOG_CAP_KB to int", "error", err)
			os.Exit(1)
		}
		scanner.Buffer(buf, int(maxlogcapKBn*1024)) // mutliplied with 1024 to make KB size

		exchangename, exists := os.LookupEnv("DIRECT_EXCHANGE_NAME")
		if !exists {
			slog.Error("Fatal: missing env var DIRECT_EXCHANGE_NAME")
			os.Exit(1)
		}

		for scanner.Scan() {
			eventPayload := scanner.Bytes()

			var eventStream utils.Event
			json.Unmarshal(eventPayload, &eventStream)

			rmqPayload := utils.RMQPayload{
				Type:    eventStream.Type,
				Status:  eventStream.Status,
				Details: eventStream.Details,
			}
			rmqData, err := json.Marshal(&rmqPayload)
			if err != nil {
				slog.Error("Failed to marshal event payload for RMQ", "error", err)
			}

			routeToRMQ(ctx, jobspec.SubmissionID, rmqm, exchangename, rmqData)

			// S3 upload happens asynchronously so uplaod doesn't block main event stream
			go func() {
				if err := s3m.UploadFileToS3(
					ctx, fmt.Sprintf("%v/result/stdout.log", jobspec.SubmissionID), strings.NewReader(eventStream.Stdout),
				); err != nil {
					// in case of error, log it and move on. Can't wait during live stream
					slog.Error("Failed to upload stdout.log to S3", "submission_id", jobspec.SubmissionID, "error", err)
				}
			}()

			go func() {
				if err := s3m.UploadFileToS3(
					ctx, fmt.Sprintf("%v/result/stderr.log", jobspec.SubmissionID), strings.NewReader(eventStream.Stderr),
				); err != nil {
					// in case of error, log it and move on. Can't wait during live stream
					slog.Error("Failed to upload stderr.log to S3", "submission_id", jobspec.SubmissionID, "error", err)
				}
			}()
		}

		if err := scanner.Err(); err != nil {
			slog.Warn("Failed to scan streamed data", "submission_id", jobspec.SubmissionID, "error", err)
			return
		}

	}(wc.Conn)

	// Handle timeouts & exit
	timeouts, _ := strconv.ParseInt(os.Getenv("TIMEOUT_SEC"), 10, 64)
	timeoutDuration := time.Duration(timeouts) * time.Second
	ctxTimeout, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	var status containerd.ExitStatus
	var stdoutWriter bytes.Buffer
	var stderrWrite bytes.Buffer
	select {
	case status = <-wc.ContStatus:

		// Process completed naturally within timeout limit
		elapsedMS := time.Since(start).Milliseconds()
		contInfo.Interval = uint64(elapsedMS)

		if status.ExitCode() == 0 {
			contInfo.Status = "OK"
			contInfo.StatusInfo = "Container exited normally"
			contInfo.ContainerStderr = stderrWrite.String()
			contInfo.ContainerStdout = stdoutWriter.String()
		} else {
			contInfo.Status = "ERROR"
			contInfo.StatusInfo = fmt.Sprintf("Container exited with code %d", status.ExitCode())
			contInfo.ContainerStderr = stderrWrite.String()
			contInfo.ContainerStdout = stdoutWriter.String()
		}
	case <-ctxTimeout.Done():
		// TLE
		elapsedMS := time.Since(start).Milliseconds()
		contInfo.Interval = uint64(elapsedMS)
		contInfo.Status = "ERROR"
		contInfo.StatusInfo = fmt.Sprintf("Task exceeded time limit of %d seconds", timeoutDuration)
		contInfo.ContainerStderr = stderrWrite.String()
		contInfo.ContainerStdout = stdoutWriter.String()

		slog.Warn("Task timed out, sending SIGKILL to container", "submission_id", jobspec.SubmissionID)
		_ = wc.Task.Kill(ctx, syscall.SIGKILL)
	}

	return contInfo
}
