// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2026 Datadog, Inc.

// Package tracerflare implements agent-triggered tracer flare support.
package tracerflare

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/DataDog/dd-trace-go/v2/internal/locking"
	"github.com/DataDog/dd-trace-go/v2/internal/log"
	"github.com/DataDog/dd-trace-go/v2/internal/remoteconfig"

	"github.com/DataDog/datadog-agent/pkg/remoteconfig/state"
)

const (
	agentConfigProduct = "AGENT_CONFIG"
	agentTaskProduct   = "AGENT_TASK"
	endpoint           = "/tracer_flare/v1"
	uploadTimeout      = 5 * time.Second
	captureMaxBytes    = 10 << 20
	maxSentUUIDs       = 64
)

var (
	staleDuration = 20 * time.Minute
	caseIDPattern = regexp.MustCompile(`^[0-9]+-(with-debug|with-content)$`)

	manager = flareManager{
		levels:    make(map[string]log.Level),
		sentUUIDs: make(map[string]struct{}),
	}
)

type flareManager struct {
	mu              locking.Mutex
	cfg             Config
	levels          map[string]log.Level
	capture         *log.Capture
	currentLevel    string
	staleTimer      *time.Timer
	timerGeneration uint64
	sentUUIDs       map[string]struct{}
	sentUUIDOrder   []string
}

// Config configures tracer flare uploads.
type Config struct {
	AgentURL   string
	HTTPClient *http.Client
	ConfigDump func() ([]byte, error)
}

// Subscribe registers the AGENT_CONFIG and AGENT_TASK remote-config products.
// The remote-config client must already be started.
func Subscribe(cfg Config) error {
	manager.mu.Lock()
	manager.cfg = cfg
	manager.mu.Unlock()

	configToken, err := remoteconfig.Subscribe(agentConfigProduct, onAgentConfig)
	if err != nil {
		return err
	}
	if _, err = remoteconfig.Subscribe(agentTaskProduct, onAgentTask); err != nil {
		_ = remoteconfig.Unsubscribe(configToken)
		return err
	}
	return nil
}

// Reset stops log capture and clears the active log-level configuration.
func Reset() {
	manager.mu.Lock()
	manager.stopCaptureLocked()
	clear(manager.levels)
	manager.currentLevel = ""
	manager.mu.Unlock()
}

func onAgentConfig(update remoteconfig.ProductUpdate) map[string]state.ApplyStatus {
	type updateValue struct {
		path  string
		level *log.Level
	}
	values := make([]updateValue, 0, len(update))
	statuses := make(map[string]state.ApplyStatus, len(update))
	for path, raw := range update {
		if len(raw) == 0 {
			values = append(values, updateValue{path: path})
			statuses[path] = state.ApplyStatus{State: state.ApplyStateAcknowledged}
			continue
		}

		var payload struct {
			Config struct {
				LogLevel *string `json:"log_level"`
			} `json:"config"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			statuses[path] = state.ApplyStatus{State: state.ApplyStateError, Error: err.Error()}
			continue
		}
		level, ok := parseLevel(payload.Config.LogLevel)
		if !ok {
			values = append(values, updateValue{path: path})
		} else {
			values = append(values, updateValue{path: path, level: &level})
		}
		statuses[path] = state.ApplyStatus{State: state.ApplyStateAcknowledged}
	}

	manager.mu.Lock()
	changed := false
	for _, value := range values {
		if value.level == nil {
			if _, found := manager.levels[value.path]; found {
				delete(manager.levels, value.path)
				changed = true
			}
		} else {
			manager.levels[value.path] = *value.level
			changed = true
		}
	}
	if changed {
		manager.updateCaptureLocked()
	}
	manager.mu.Unlock()
	return statuses
}

func parseLevel(value *string) (log.Level, bool) {
	if value == nil {
		return 0, false
	}
	switch strings.ToLower(*value) {
	case "trace", "debug":
		return log.LevelDebug, true
	case "info":
		return log.LevelInfo, true
	case "warn", "warning":
		return log.LevelWarn, true
	case "error", "critical":
		return log.LevelError, true
	default:
		log.Debug("tracer flare: ignoring unknown log level %q", *value)
		return 0, false
	}
}

func (m *flareManager) updateCaptureLocked() {
	if len(m.levels) == 0 {
		m.stopCaptureLocked()
		m.currentLevel = ""
		return
	}

	level := log.LevelError
	for _, configuredLevel := range m.levels {
		if configuredLevel < level {
			level = configuredLevel
		}
	}
	m.currentLevel = strings.ToLower(level.String())
	if m.capture == nil {
		m.capture = log.StartCapture(level, captureMaxBytes)
	} else {
		m.capture.SetLevel(level)
	}
	m.armStaleTimerLocked()
}

func (m *flareManager) armStaleTimerLocked() {
	m.cancelStaleTimerLocked()
	generation := m.timerGeneration
	m.staleTimer = time.AfterFunc(staleDuration, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if generation != m.timerGeneration {
			return
		}
		m.staleTimer = nil
		m.timerGeneration++
		if m.capture != nil {
			m.capture.Stop()
			m.capture = nil
		}
	})
}

func (m *flareManager) cancelStaleTimerLocked() {
	m.timerGeneration++
	if m.staleTimer != nil {
		m.staleTimer.Stop()
		m.staleTimer = nil
	}
}

func (m *flareManager) stopCaptureLocked() []byte {
	m.cancelStaleTimerLocked()
	if m.capture == nil {
		return nil
	}
	captured := m.capture.Stop()
	m.capture = nil
	return captured
}

type agentTask struct {
	Args struct {
		CaseID     string `json:"case_id"`
		Hostname   string `json:"hostname"`
		UserHandle string `json:"user_handle"`
	} `json:"args"`
	TaskType string `json:"task_type"`
	UUID     string `json:"uuid"`
}

func onAgentTask(update remoteconfig.ProductUpdate) map[string]state.ApplyStatus {
	statuses := make(map[string]state.ApplyStatus, len(update))
	for path, raw := range update {
		if len(raw) == 0 {
			statuses[path] = state.ApplyStatus{State: state.ApplyStateAcknowledged}
			continue
		}
		var task agentTask
		if err := json.Unmarshal(raw, &task); err != nil {
			statuses[path] = state.ApplyStatus{State: state.ApplyStateError, Error: err.Error()}
			continue
		}
		if task.TaskType != "tracer_flare" {
			log.Debug("tracer flare: ignoring agent task type %q", task.TaskType)
			statuses[path] = state.ApplyStatus{State: state.ApplyStateAcknowledged}
			continue
		}
		if !validCaseID(task.Args.CaseID) {
			statuses[path] = state.ApplyStatus{State: state.ApplyStateError, Error: "invalid case_id"}
			continue
		}

		manager.mu.Lock()
		if manager.hasSentUUIDLocked(task.UUID) {
			manager.mu.Unlock()
			statuses[path] = state.ApplyStatus{State: state.ApplyStateAcknowledged}
			continue
		}
		manager.rememberUUIDLocked(task.UUID)
		captured := manager.stopCaptureLocked()
		cfg := manager.cfg
		level := manager.currentLevel
		manager.mu.Unlock()

		if level == "" {
			level = "debug"
		}
		if err := upload(cfg, task, level, captured); err != nil {
			log.Warn("tracer flare: failed to upload flare: %s", err.Error())
			manager.mu.Lock()
			manager.forgetUUIDLocked(task.UUID)
			manager.mu.Unlock()
		}
		statuses[path] = state.ApplyStatus{State: state.ApplyStateAcknowledged}
	}
	return statuses
}

func validCaseID(value string) bool {
	if value == "" || value == "0" {
		return false
	}
	allDigits := true
	for _, char := range value {
		if char < '0' || char > '9' {
			allDigits = false
			break
		}
	}
	return allDigits || caseIDPattern.MatchString(value)
}

func (m *flareManager) hasSentUUIDLocked(uuid string) bool {
	_, found := m.sentUUIDs[uuid]
	return found
}

func (m *flareManager) rememberUUIDLocked(uuid string) {
	if len(m.sentUUIDOrder) == maxSentUUIDs {
		delete(m.sentUUIDs, m.sentUUIDOrder[0])
		m.sentUUIDOrder = m.sentUUIDOrder[1:]
	}
	m.sentUUIDs[uuid] = struct{}{}
	m.sentUUIDOrder = append(m.sentUUIDOrder, uuid)
}

func (m *flareManager) forgetUUIDLocked(uuid string) {
	delete(m.sentUUIDs, uuid)
	for i, sentUUID := range m.sentUUIDOrder {
		if sentUUID == uuid {
			m.sentUUIDOrder = append(m.sentUUIDOrder[:i], m.sentUUIDOrder[i+1:]...)
			return
		}
	}
}

func upload(cfg Config, task agentTask, level string, captured []byte) error {
	configFile := tracerConfig(cfg.ConfigDump)
	zipBytes, err := buildZip(captured, configFile)
	if err != nil {
		return err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "source", value: "tracer_go"},
		{name: "case_id", value: task.Args.CaseID},
		{name: "hostname", value: task.Args.Hostname},
		{name: "email", value: task.Args.UserHandle},
		{name: "uuid", value: task.UUID},
	} {
		if err := writer.WriteField(field.name, field.value); err != nil {
			return err
		}
	}
	filename := fmt.Sprintf("tracer-go-%s-%d-%s.zip", task.Args.CaseID, time.Now().Unix(), level)
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="flare_file"; filename="%s"`, filename))
	header.Set("Content-Type", "application/octet-stream")
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	if _, err := part.Write(zipBytes); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	requestContext, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()
	url := strings.TrimRight(cfg.AgentURL, "/") + endpoint
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, url, &body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("agent returned HTTP status %d", response.StatusCode)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	return nil
}

func tracerConfig(configDump func() ([]byte, error)) []byte {
	configs := json.RawMessage("{}")
	var dumpErr error
	if configDump == nil {
		dumpErr = errors.New("tracer configuration is unavailable")
	} else if data, err := configDump(); err != nil {
		dumpErr = err
	} else if !json.Valid(data) {
		dumpErr = errors.New("tracer configuration is not valid JSON")
	} else {
		configs = data
	}

	payload := struct {
		Configs json.RawMessage `json:"configs"`
		Error   string          `json:"error,omitempty"`
	}{Configs: configs}
	if dumpErr != nil {
		payload.Error = dumpErr.Error()
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return []byte(`{"configs":{},"error":"failed to encode tracer configuration"}`)
	}
	return encoded
}

func buildZip(logs, config []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, file := range []struct {
		name string
		data []byte
	}{
		{name: fmt.Sprintf("tracer_go_%d.log", os.Getpid()), data: logs},
		{name: fmt.Sprintf("tracer_config_%d.json", os.Getpid()), data: config},
	} {
		part, err := writer.Create(file.name)
		if err != nil {
			return nil, err
		}
		if _, err := part.Write(file.data); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
