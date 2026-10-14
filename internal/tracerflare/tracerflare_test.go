// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2026 Datadog, Inc.

package tracerflare

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DataDog/dd-trace-go/v2/internal/log"
	"github.com/DataDog/dd-trace-go/v2/internal/remoteconfig"

	"github.com/DataDog/datadog-agent/pkg/remoteconfig/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetManager(t *testing.T) {
	t.Helper()
	previousLevel := log.GetLevel()
	restoreLogger := log.UseLogger(log.DiscardLogger{})
	reset := func() {
		manager.mu.Lock()
		manager.cancelStaleTimerLocked()
		if manager.capture != nil {
			manager.capture.Stop()
		}
		manager.cfg = Config{}
		manager.levels = make(map[string]log.Level)
		manager.capture = nil
		manager.currentLevel = ""
		manager.sentUUIDs = make(map[string]struct{})
		manager.sentUUIDOrder = nil
		manager.mu.Unlock()
		log.SetLevel(previousLevel)
	}
	reset()
	t.Cleanup(func() {
		reset()
		restoreLogger()
	})
	log.SetLevel(log.LevelWarn)
}

func configUpdate(path, level string) remoteconfig.ProductUpdate {
	payload := []byte(`{"config":{"log_level":"` + level + `"}}`)
	return remoteconfig.ProductUpdate{path: payload}
}

func taskUpdate(path, caseID, taskType, uuid string) remoteconfig.ProductUpdate {
	payload, _ := json.Marshal(agentTask{
		Args: struct {
			CaseID     string `json:"case_id"`
			Hostname   string `json:"hostname"`
			UserHandle string `json:"user_handle"`
		}{CaseID: caseID, Hostname: "test-host", UserHandle: "test@example.com"},
		TaskType: taskType,
		UUID:     uuid,
	})
	return remoteconfig.ProductUpdate{path: payload}
}

func setConfig(cfg Config) {
	manager.mu.Lock()
	manager.cfg = cfg
	manager.mu.Unlock()
}

func TestAgentConfigCaptureAndRemoval(t *testing.T) {
	resetManager(t)
	require.Equal(t, state.ApplyStateAcknowledged, onAgentConfig(configUpdate("flare", "debug"))["flare"].State)
	assert.Equal(t, log.LevelDebug, log.GetLevel())

	status := onAgentConfig(remoteconfig.ProductUpdate{"flare": []byte("{}")})
	assert.Equal(t, state.ApplyStateAcknowledged, status["flare"].State)
	assert.Equal(t, log.LevelWarn, log.GetLevel())
	assert.Nil(t, manager.capture)

	onAgentConfig(configUpdate("flare", "debug"))
	status = onAgentConfig(remoteconfig.ProductUpdate{"flare": nil})
	assert.Equal(t, state.ApplyStateAcknowledged, status["flare"].State)
	assert.Equal(t, log.LevelWarn, log.GetLevel())
	assert.Nil(t, manager.capture)
}

func TestAgentConfigConfigurationOrderDoesNotChangeState(t *testing.T) {
	resetManager(t)
	onAgentConfig(configUpdate("flare", "debug"))
	activeCapture := manager.capture
	activeTimer := manager.staleTimer
	activeGeneration := manager.timerGeneration
	require.NotNil(t, activeCapture)

	status := onAgentConfig(remoteconfig.ProductUpdate{
		"datadog/1/AGENT_CONFIG/configuration_order/config": []byte(`{"order":[],"internal_order":[]}`),
	})
	assert.Equal(t, state.ApplyStateAcknowledged, status["datadog/1/AGENT_CONFIG/configuration_order/config"].State)
	assert.Same(t, activeCapture, manager.capture)
	assert.Same(t, activeTimer, manager.staleTimer)
	assert.Equal(t, activeGeneration, manager.timerGeneration)
	assert.Equal(t, map[string]log.Level{"flare": log.LevelDebug}, manager.levels)
	assert.Equal(t, log.LevelDebug, log.GetLevel())
}

func TestAgentConfigMalformedJSON(t *testing.T) {
	resetManager(t)
	status := onAgentConfig(remoteconfig.ProductUpdate{"bad": []byte("{")})
	assert.Equal(t, state.ApplyStateError, status["bad"].State)
	assert.NotEmpty(t, status["bad"].Error)
}

func TestAgentTaskUploadsFlare(t *testing.T) {
	resetManager(t)
	onAgentConfig(configUpdate("flare", "debug"))
	log.Debug("pre-task debug line")

	fields := make(map[string]string)
	var fieldOrder []string
	var fileName, fileContentType string
	var archive []byte
	var handlerErr error
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		if r.URL.Path != endpoint {
			handlerErr = io.ErrUnexpectedEOF
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reader, err := r.MultipartReader()
		if err != nil {
			handlerErr = err
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				handlerErr = err
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			name := part.FormName()
			fieldOrder = append(fieldOrder, name)
			data, err := io.ReadAll(part)
			if err != nil {
				handlerErr = err
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if name == "flare_file" {
				fileName = part.FileName()
				fileContentType = part.Header.Get("Content-Type")
				archive = data
			} else {
				fields[name] = string(data)
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	setConfig(Config{
		AgentURL:   server.URL,
		HTTPClient: server.Client(),
		ConfigDump: func() ([]byte, error) { return []byte(`{"service":"test-service"}`), nil },
	})

	status := onAgentTask(taskUpdate("task", "12345", "tracer_flare", "uuid-1"))
	<-handlerDone
	assert.Equal(t, state.ApplyStateAcknowledged, status["task"].State)
	assert.NoError(t, handlerErr)
	assert.Equal(t, []string{"source", "case_id", "hostname", "email", "uuid", "flare_file"}, fieldOrder)
	assert.Equal(t, map[string]string{
		"source":   "tracer_go",
		"case_id":  "12345",
		"hostname": "test-host",
		"email":    "test@example.com",
		"uuid":     "uuid-1",
	}, fields)
	assert.True(t, strings.HasPrefix(fileName, "tracer-go-12345-"))
	assert.True(t, strings.HasSuffix(fileName, "-debug.zip"))
	assert.Equal(t, "application/octet-stream", fileContentType)

	reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	require.NoError(t, err)
	entries := make(map[string][]byte, len(reader.File))
	for _, file := range reader.File {
		part, err := file.Open()
		require.NoError(t, err)
		contents, err := io.ReadAll(part)
		require.NoError(t, err)
		require.NoError(t, part.Close())
		entries[file.Name] = contents
	}
	logName := "tracer_go_" + strconv.Itoa(os.Getpid()) + ".log"
	configName := "tracer_config_" + strconv.Itoa(os.Getpid()) + ".json"
	assert.Contains(t, string(entries[logName]), "pre-task debug line")
	assert.JSONEq(t, `{"configs":{"service":"test-service"}}`, string(entries[configName]))
}

func TestAgentTaskNonTracerFlareDoesNotUpload(t *testing.T) {
	resetManager(t)
	var uploads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		uploads.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	setConfig(Config{AgentURL: server.URL, HTTPClient: server.Client()})

	status := onAgentTask(taskUpdate("task", "123", "flare", "uuid-1"))
	assert.Equal(t, state.ApplyStateAcknowledged, status["task"].State)
	assert.Zero(t, uploads.Load())
}

func TestAgentTaskMalformedJSON(t *testing.T) {
	resetManager(t)
	status := onAgentTask(remoteconfig.ProductUpdate{"bad": []byte("{")})
	assert.Equal(t, state.ApplyStateError, status["bad"].State)
	assert.NotEmpty(t, status["bad"].Error)
}

func TestAgentTaskInvalidCaseIDDoesNotUpload(t *testing.T) {
	for _, caseID := range []string{"0", "abc"} {
		t.Run(caseID, func(t *testing.T) {
			resetManager(t)
			var uploads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				uploads.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			setConfig(Config{AgentURL: server.URL, HTTPClient: server.Client()})

			status := onAgentTask(taskUpdate("task", caseID, "tracer_flare", "uuid-1"))
			assert.Equal(t, state.ApplyStateError, status["task"].State)
			assert.Equal(t, "invalid case_id", status["task"].Error)
			assert.Zero(t, uploads.Load())
		})
	}
}

func TestAgentTaskDuplicateUUIDUploadsOnce(t *testing.T) {
	resetManager(t)
	var uploads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		uploads.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	setConfig(Config{AgentURL: server.URL, HTTPClient: server.Client()})

	first := onAgentTask(taskUpdate("task-1", "123", "tracer_flare", "same-uuid"))
	second := onAgentTask(taskUpdate("task-2", "123", "tracer_flare", "same-uuid"))
	assert.Equal(t, state.ApplyStateAcknowledged, first["task-1"].State)
	assert.Equal(t, state.ApplyStateAcknowledged, second["task-2"].State)
	assert.EqualValues(t, 1, uploads.Load())
}

func TestAgentTaskUploadFailureStillAcknowledges(t *testing.T) {
	resetManager(t)
	var uploads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		uploads.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	setConfig(Config{AgentURL: server.URL, HTTPClient: server.Client()})

	status := onAgentTask(taskUpdate("task", "123", "tracer_flare", "uuid-1"))
	assert.Equal(t, state.ApplyStateAcknowledged, status["task"].State)
	assert.EqualValues(t, 1, uploads.Load())
}

func TestAgentConfigStaleTimerRestoresLevel(t *testing.T) {
	resetManager(t)
	previousDuration := staleDuration
	staleDuration = 10 * time.Millisecond
	t.Cleanup(func() { staleDuration = previousDuration })

	onAgentConfig(configUpdate("flare", "debug"))
	require.Equal(t, log.LevelDebug, log.GetLevel())
	require.Eventually(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.capture == nil && log.GetLevel() == log.LevelWarn
	}, time.Second, time.Millisecond)
}

func TestBuildZipConfigFallback(t *testing.T) {
	config := tracerConfig(nil)
	assert.JSONEq(t, `{"configs":{},"error":"tracer configuration is unavailable"}`, string(config))

	config = tracerConfig(func() ([]byte, error) { return nil, errors.New("configuration dump failed") })
	assert.JSONEq(t, `{"configs":{},"error":"configuration dump failed"}`, string(config))
}
