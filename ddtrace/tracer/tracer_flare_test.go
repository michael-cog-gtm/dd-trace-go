// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2026 Datadog, Inc.

package tracer

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/dd-trace-go/v2/internal/remoteconfig"
)

func TestStartSubscribesTracerFlareRemoteConfig(t *testing.T) {
	for _, test := range []struct {
		name            string
		tracingDisabled bool
	}{
		{name: "tracing enabled"},
		{name: "tracing disabled", tracingDisabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := startTestAgent(t)
			agent.SetInfo(`{"endpoints":["/v0.7/config"]}`)
			t.Setenv("DD_REMOTE_CONFIGURATION_ENABLED", "true")
			if test.tracingDisabled {
				t.Setenv("DD_TRACE_ENABLED", "false")
			} else {
				t.Setenv("DD_TRACE_ENABLED", "true")
			}

			Stop()
			t.Cleanup(Stop)
			require.NoError(t, Start(WithAgentAddr(agent.Addr())))

			hasFlareProducts := func() bool {
				for _, product := range []string{"AGENT_CONFIG", "AGENT_TASK"} {
					found, err := remoteconfig.HasProduct(product)
					if err != nil || !found {
						return false
					}
				}
				return true
			}
			if test.tracingDisabled {
				require.Eventually(t, func() bool {
					flareRC.mu.Lock()
					started := flareRC.started
					flareRC.mu.Unlock()
					return started && hasFlareProducts()
				}, time.Second, 10*time.Millisecond)
			} else {
				require.True(t, hasFlareProducts())
			}
			if !test.tracingDisabled {
				found, err := remoteconfig.HasProduct("APM_TRACING")
				require.NoError(t, err)
				require.True(t, found)
			}
			if test.tracingDisabled {
				found, err := remoteconfig.HasCapability(remoteconfig.APMTracingSampleRate)
				require.NoError(t, err)
				require.False(t, found)
			}

			Stop()
			for _, product := range []string{"AGENT_CONFIG", "AGENT_TASK"} {
				_, err := remoteconfig.HasProduct(product)
				require.ErrorIs(t, err, remoteconfig.ErrClientNotStarted)
			}
		})
	}
}

func TestStartFlareOnlySkipsUnsupportedAgent(t *testing.T) {
	agent := startTestAgent(t)
	agent.SetInfo(`{"endpoints":[]}`)
	t.Setenv("DD_REMOTE_CONFIGURATION_ENABLED", "true")
	t.Setenv("DD_TRACE_ENABLED", "false")

	Stop()
	t.Cleanup(Stop)
	require.NoError(t, Start(WithAgentAddr(agent.Addr())))
	_, err := remoteconfig.HasProduct("AGENT_CONFIG")
	require.ErrorIs(t, err, remoteconfig.ErrClientNotStarted)
}

func TestStartFlareOnlyReturnsQuicklyForHungAgentInfo(t *testing.T) {
	t.Setenv("DD_REMOTE_CONFIGURATION_ENABLED", "true")
	t.Setenv("DD_TRACE_ENABLED", "false")
	Stop()

	release := make(chan struct{})
	infoStarted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/info" {
			http.NotFound(w, r)
			return
		}
		select {
		case infoStarted <- struct{}{}:
		default:
		}
		<-release
		_, _ = w.Write([]byte(`{"endpoints":["/v0.7/config"]}`))
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
		Stop()
	})

	startedAt := time.Now()
	require.NoError(t, Start(WithAgentAddr(server.Listener.Addr().String())))
	require.Less(t, time.Since(startedAt), 500*time.Millisecond)
	select {
	case <-infoStarted:
	case <-time.After(time.Second):
		t.Fatal("agent /info request did not start")
	}

	stoppedAt := time.Now()
	Stop()
	require.Less(t, time.Since(stoppedAt), 500*time.Millisecond)
	_, err := remoteconfig.HasProduct("AGENT_CONFIG")
	require.ErrorIs(t, err, remoteconfig.ErrClientNotStarted)
}

func TestFlareOnlyConcurrentStartStop(t *testing.T) {
	agent := startTestAgent(t)
	agent.SetInfo(`{"endpoints":["/v0.7/config"]}`)
	t.Setenv("DD_REMOTE_CONFIGURATION_ENABLED", "true")
	t.Setenv("DD_TRACE_ENABLED", "false")
	Stop()
	stopOnCleanup := true
	t.Cleanup(func() {
		if stopOnCleanup {
			Stop()
		}
	})

	const (
		goroutines = 4
		iterations = 20
	)
	errs := make(chan error, goroutines*iterations)
	finished := make(chan struct{}, goroutines)
	for range goroutines {
		go func() {
			defer func() { finished <- struct{}{} }()
			for range iterations {
				if err := Start(WithAgentAddr(agent.Addr())); err != nil {
					errs <- err
				}
				Stop()
			}
		}()
	}

	timeout := time.NewTimer(20 * time.Second)
	defer timeout.Stop()
	for range goroutines {
		select {
		case <-finished:
		case <-timeout.C:
			stopOnCleanup = false
			t.Fatal("concurrent Start/Stop did not complete within 20s")
		}
	}
	Stop()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
