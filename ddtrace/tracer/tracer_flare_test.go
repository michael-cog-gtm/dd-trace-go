// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2026 Datadog, Inc.

package tracer

import (
	"testing"

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

			for _, product := range []string{"AGENT_CONFIG", "AGENT_TASK"} {
				found, err := remoteconfig.HasProduct(product)
				require.NoError(t, err)
				require.True(t, found, "expected %s subscription", product)
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
			_, err := remoteconfig.HasProduct("AGENT_CONFIG")
			require.ErrorIs(t, err, remoteconfig.ErrClientNotStarted)
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
