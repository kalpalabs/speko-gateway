package kalpa

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	testVoice = "18c26f49-9f9c-400c-bec9-7467f28e1db5"
	testKey   = "customer-kalpa-key"
)

func TestStreamsAnUtterance(t *testing.T) {
	t.Parallel()
	pcm := []byte{1, 0, 2, 0, 3, 0, 4, 0}
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		init, err := readFrame(ctx, conn)
		if err != nil {
			t.Errorf("read init: %v", err)
			return
		}
		want := map[string]any{
			"type": "initializeConnection", "api_key": testKey, "model": "kalpa-tts-1",
			"output_format": "pcm_s16le", "sample_rate": float64(16_000), "generation_config": map[string]any{},
		}
		if !reflect.DeepEqual(init, want) {
			t.Errorf("init = %#v, want %#v", init, want)
		}
		_ = writeJSON(ctx, conn, sessionCreated(16_000))
		for _, want := range []map[string]any{
			{"type": "sendText", "text": "Hello "},
			{"type": "sendText", "text": "there."},
			{"type": "sendText", "text": "", "flush": true},
		} {
			if frame, err := readFrame(ctx, conn); err != nil || !reflect.DeepEqual(frame, want) {
				t.Errorf("frame = %#v (%v), want %#v", frame, err, want)
				return
			}
		}
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_1", "sample_rate": 16_000})
		_ = writeJSON(ctx, conn, audioFrame("resp_1", nil))
		_ = writeJSON(ctx, conn, audioFrame("resp_1", pcm[:4]))
		_ = writeJSON(ctx, conn, audioFrame("resp_1", pcm[4:]))
		_ = writeJSON(ctx, conn, responseDone("resp_1", "completed", 1.2346))
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	stream := openStream(t, server.URL, 16_000)
	events := stream.Events()
	if event := waitEvent(t, events); event.Type != protocol.EventUsageObserved || decodeString(t, event.Data, "provider_request_id") != "sess_1" {
		t.Fatalf("first event = %s %s, want usage.observed for the session", event.Type, event.Data)
	}
	ctx := context.Background()
	for _, text := range []string{"Hello ", "there."} {
		if err := stream.AppendText(ctx, text); err != nil {
			t.Fatal(err)
		}
	}
	if err := stream.CommitText(ctx); err != nil {
		t.Fatal(err)
	}
	if event := waitEvent(t, events); event.Type != protocol.EventAudioStarted {
		t.Fatalf("event = %s, want audio.started", event.Type)
	}
	var audio []byte
	for range 2 {
		event := waitEvent(t, events)
		if event.Type != protocol.EventAudioFrame || decodeString(t, event.Data, "provider_request_id") != "resp_1" {
			t.Fatalf("event = %s %s, want an audio.frame of resp_1", event.Type, event.Data)
		}
		audio = append(audio, event.Audio...)
	}
	if string(audio) != string(pcm) {
		t.Fatalf("audio = %v, want %v", audio, pcm)
	}
	done := waitEvent(t, events)
	if done.Type != protocol.EventAudioDone || decodeBool(t, done.Data, "cancelled") {
		t.Fatalf("event = %s %s, want a completed audio.done", done.Type, done.Data)
	}
	if done.Billing == nil || done.Billing.OperationID != "resp_1" || done.Billing.Model != "kalpa-tts-1" || !done.Billing.Complete ||
		done.Billing.Quantities["duration_seconds"] != 1235 {
		t.Fatalf("billing = %#v, want 1.235 s for resp_1", done.Billing)
	}
	if err := done.Billing.Validate(); err != nil {
		t.Fatalf("billing invalid: %v", err)
	}
	if err := stream.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestOpenClassifiesRefusals(t *testing.T) {
	t.Parallel()
	refuse := func(kind string, close websocket.StatusCode) func(context.Context, *websocket.Conn) {
		return func(ctx context.Context, conn *websocket.Conn) {
			_, _ = readFrame(ctx, conn)
			_ = writeJSON(ctx, conn, map[string]any{"type": "error", "fatal": true, "error": map[string]any{"type": kind, "message": "refused"}})
			_ = conn.Close(close, "")
		}
	}
	tests := []struct {
		name      string
		serve     func(context.Context, *websocket.Conn)
		code      string
		retryable bool
	}{
		{"bad key", refuse("authentication_error", 4401), "authentication_failed", false},
		{"allowance used up", refuse("allowance_exhausted", 4402), "provider_quota_exceeded", false},
		{"unknown voice", refuse("not_found", 4404), "invalid_request", false},
		{"key store down", refuse("service_unavailable", 4503), "provider_unavailable", true},
		{"close without a frame", func(ctx context.Context, conn *websocket.Conn) {
			_, _ = readFrame(ctx, conn)
			_ = conn.Close(4408, "idle timeout")
		}, "request_timeout", true},
		{"no answer", func(ctx context.Context, conn *websocket.Conn) {
			_, _ = readFrame(ctx, conn)
			_, _, _ = conn.Read(ctx)
		}, "request_timeout", true},
		{"another sample rate", func(ctx context.Context, conn *websocket.Conn) {
			_, _ = readFrame(ctx, conn)
			_ = writeJSON(ctx, conn, sessionCreated(24_000))
		}, "unsupported_media", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := newSocketServer(t, test.serve)
			defer server.Close()
			config := ttsConfig(server.URL)
			config.HandshakeTimeout = 200 * time.Millisecond
			adapter, err := NewTTS(config)
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Open(context.Background(), ttsRequest(server.URL, 16_000))
			assertProviderError(t, err, test.code, test.retryable)
		})
	}

	for status, want := range map[int]string{http.StatusForbidden: "invalid_request", http.StatusServiceUnavailable: "provider_unavailable"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		adapter, _ := NewTTS(ttsConfig(server.URL))
		_, err := adapter.Open(context.Background(), ttsRequest(server.URL, 16_000))
		assertProviderError(t, err, want, status >= 500)
		server.Close()
	}
}

func TestOpenRejectsUnroutableRequests(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("an unroutable request must not dial")
	}))
	defer server.Close()
	tests := map[string]func(*runtimepkg.AdapterRequest){
		"stt kind":          func(r *runtimepkg.AdapterRequest) { r.Kind = protocol.SessionKindSTT },
		"auto model":        func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Model = "auto" },
		"managed direct":    func(r *runtimepkg.AdapterRequest) { r.Plan.Execution.CredentialSource = protocol.CredentialsManaged },
		"opus":              func(r *runtimepkg.AdapterRequest) { r.Media.Encoding = "opus" },
		"stereo":            func(r *runtimepkg.AdapterRequest) { r.Media.Channels = 2 },
		"11025 Hz":          func(r *runtimepkg.AdapterRequest) { r.Media.SampleRateHz = 11_025 },
		"no voice":          func(r *runtimepkg.AdapterRequest) { r.Options.Voice = ""; r.Plan.Route.Voice = "" },
		"voice with a path": func(r *runtimepkg.AdapterRequest) { r.Options.Voice = "../v1/converse" },
		"endpoint path":     func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Endpoint = endpointFor(server.URL, "/v1/converse") },
		"no credential":     func(r *runtimepkg.AdapterRequest) { r.Plan.Route.Credential = nil },
	}
	adapter, err := NewTTS(ttsConfig(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range tests {
		request := ttsRequest(server.URL, 16_000)
		mutate(&request)
		if _, err := adapter.Open(context.Background(), request); err == nil {
			t.Errorf("%s: Open succeeded", name)
		} else if strings.Contains(err.Error(), testKey) {
			t.Errorf("%s: error leaks the credential: %v", name, err)
		}
	}
	request := ttsRequest(server.URL, 16_000)
	request.Options.Language = "hi"
	request.Media.SampleRateHz = 44_100
	request.Plan.Route.Endpoint = endpointFor(server.URL, "/v1/converse")
	_, err = adapter.Open(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "endpoint path") {
		t.Fatalf("hi at 44.1 kHz must pass media and language checks, got %v", err)
	}
	request = ttsRequest(server.URL, 16_000)
	request.Options.Language = "fr"
	_, err = adapter.Open(context.Background(), request)
	assertProviderError(t, err, "unsupported_language", false)
}

// TestCancelDropsStoppedWorkAndReusesTheSocket covers the barge-in path: the
// next utterance is sent before the acknowledgement arrives, and nothing of the
// stopped response, including a late audio chunk and its own error, reaches
// the caller.
func TestCancelDropsStoppedWorkAndReusesTheSocket(t *testing.T) {
	t.Parallel()
	cancelled := make(chan struct{})
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		expectFrames(ctx, t, conn, map[string]any{"type": "sendText", "text": "A."}, map[string]any{"type": "sendText", "text": "", "flush": true})
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_a"})
		_ = writeJSON(ctx, conn, audioFrame("resp_a", []byte{1, 0}))
		expectFrames(ctx, t, conn, map[string]any{"type": "cancelResponse"})
		close(cancelled)
		expectFrames(ctx, t, conn, map[string]any{"type": "sendText", "text": "B."}, map[string]any{"type": "sendText", "text": "", "flush": true})
		_ = writeJSON(ctx, conn, audioFrame("resp_a", []byte{9, 9}))
		_ = writeJSON(ctx, conn, map[string]any{"type": "error", "fatal": false, "response_id": "resp_a", "error": map[string]any{"type": "inference_error"}})
		_ = writeJSON(ctx, conn, responseDone("resp_a", "cancelled", 0.1))
		_ = writeJSON(ctx, conn, map[string]any{"type": "cancelled"})
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_b"})
		_ = writeJSON(ctx, conn, audioFrame("resp_b", []byte{2, 0}))
		_ = writeJSON(ctx, conn, responseDone("resp_b", "completed", 0.5))
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	stream := openStream(t, server.URL, 24_000)
	events := stream.Events()
	waitEvent(t, events) // usage.observed
	ctx := context.Background()
	mustSay(t, stream, "A.")
	expectEvents(t, events, protocol.EventAudioStarted, protocol.EventAudioFrame)
	if err := stream.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	<-cancelled
	mustSay(t, stream, "B.")
	done := waitEvent(t, events)
	if done.Type != protocol.EventAudioDone || !decodeBool(t, done.Data, "cancelled") || decodeString(t, done.Data, "provider_request_id") != "resp_a" || done.Billing != nil {
		t.Fatalf("event = %s %s billing=%v, want resp_a's cancelled audio.done", done.Type, done.Data, done.Billing)
	}
	expectEvents(t, events, protocol.EventAudioStarted)
	if frame := waitEvent(t, events); frame.Type != protocol.EventAudioFrame || string(frame.Audio) != string([]byte{2, 0}) {
		t.Fatalf("event = %s %v, want resp_b's audio", frame.Type, frame.Audio)
	}
	if done := waitEvent(t, events); done.Type != protocol.EventAudioDone || decodeBool(t, done.Data, "cancelled") {
		t.Fatalf("event = %s %s, want resp_b completed", done.Type, done.Data)
	}
}

// TestCancelBeforeTheResponseStarts covers the common barge-in race: the
// cancel reaches Kalpa before responseCreated, and the stopped response had in
// fact completed, so Kalpa bills it. The stopped utterance's audio.done waits
// for the ack and carries that billing; the next utterance plays in full.
func TestCancelBeforeTheResponseStarts(t *testing.T) {
	t.Parallel()
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		expectFrames(ctx, t, conn,
			map[string]any{"type": "sendText", "text": "A."}, map[string]any{"type": "sendText", "text": "", "flush": true},
			map[string]any{"type": "cancelResponse"},
			map[string]any{"type": "sendText", "text": "B."}, map[string]any{"type": "sendText", "text": "", "flush": true})
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_a"})
		_ = writeJSON(ctx, conn, responseDone("resp_a", "completed", 0.5))
		_ = writeJSON(ctx, conn, map[string]any{"type": "cancelled"})
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_b"})
		_ = writeJSON(ctx, conn, audioFrame("resp_b", []byte{2, 0}))
		_ = writeJSON(ctx, conn, responseDone("resp_b", "completed", 0.25))
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	stream := openStream(t, server.URL, 24_000)
	events := stream.Events()
	waitEvent(t, events)
	mustSay(t, stream, "A.")
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustSay(t, stream, "B.")
	stopped := waitEvent(t, events)
	if stopped.Type != protocol.EventAudioDone || !decodeBool(t, stopped.Data, "cancelled") || decodeString(t, stopped.Data, "provider_request_id") != "resp_a" {
		t.Fatalf("event = %s %s, want resp_a's cancelled audio.done", stopped.Type, stopped.Data)
	}
	if stopped.Billing == nil || stopped.Billing.Quantities["duration_seconds"] != 500 {
		t.Fatalf("billing = %#v, want the completed 0.5 s", stopped.Billing)
	}
	expectEvents(t, events, protocol.EventAudioStarted, protocol.EventAudioFrame)
	if done := waitEvent(t, events); done.Type != protocol.EventAudioDone || decodeString(t, done.Data, "provider_request_id") != "resp_b" {
		t.Fatalf("event = %s %s, want resp_b's audio.done", done.Type, done.Data)
	}
}

func TestCancelWithNothingInFlightSendsNothing(t *testing.T) {
	t.Parallel()
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		expectFrames(ctx, t, conn, map[string]any{"type": "sendText", "text": "", "flush": true})
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_1"})
		_ = writeJSON(ctx, conn, responseDone("resp_1", "completed", 0))
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	stream := openStream(t, server.URL, 24_000)
	events := stream.Events()
	waitEvent(t, events)
	if err := stream.Cancel(context.Background()); err != nil {
		t.Fatalf("idle cancel: %v", err)
	}
	// An empty commit is a completed utterance with no audio, not an error.
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
	if done := waitEvent(t, events); done.Type != protocol.EventAudioDone || decodeBool(t, done.Data, "cancelled") {
		t.Fatalf("event = %s %s, want a completed audio.done", done.Type, done.Data)
	}
}

func TestOneCommittedUtteranceAtATime(t *testing.T) {
	t.Parallel()
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		expectFrames(ctx, t, conn, map[string]any{"type": "sendText", "text": "Hi."}, map[string]any{"type": "sendText", "text": "", "flush": true})
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	stream := openStream(t, server.URL, 24_000)
	if err := stream.AppendText(context.Background(), strings.Repeat("a", maxUtteranceChars+1)); err == nil {
		t.Fatal("an over-limit utterance was accepted")
	} else {
		assertProviderError(t, err, "input_too_large", false)
	}
	mustSay(t, stream, "Hi.")
	if err := stream.AppendText(context.Background(), "Next."); err == nil {
		t.Fatal("text for the next utterance was accepted before audio.done")
	}
	if err := stream.CommitText(context.Background()); err == nil {
		t.Fatal("a second commit was accepted before audio.done")
	}
}

func TestErrorsEndTheSession(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		fail      func(context.Context, *websocket.Conn)
		code      string
		retryable bool
	}{
		{"overloaded", func(ctx context.Context, conn *websocket.Conn) {
			_ = writeJSON(ctx, conn, map[string]any{"type": "error", "fatal": false, "response_id": "resp_1", "error": map[string]any{"type": "overloaded", "message": "The model is temporarily overloaded."}})
		}, "provider_unavailable", true},
		{"context length", func(ctx context.Context, conn *websocket.Conn) {
			_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_1"})
			_ = writeJSON(ctx, conn, map[string]any{"type": "error", "fatal": false, "response_id": "resp_1", "error": map[string]any{"type": "context_length_exceeded"}})
		}, "input_too_large", false},
		{"server error close", func(ctx context.Context, conn *websocket.Conn) {
			_ = conn.Close(websocket.StatusInternalError, "internal error")
		}, "provider_unavailable", true},
		{"normal close mid-utterance", func(ctx context.Context, conn *websocket.Conn) {
			_ = conn.Close(websocket.StatusNormalClosure, "")
		}, "provider_unavailable", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
				handshakeOK(ctx, t, conn, 24_000)
				expectFrames(ctx, t, conn, map[string]any{"type": "sendText", "text": "Hi."}, map[string]any{"type": "sendText", "text": "", "flush": true})
				test.fail(ctx, conn)
				_, _, _ = conn.Read(ctx)
			})
			defer server.Close()
			stream := openStream(t, server.URL, 24_000)
			mustSay(t, stream, "Hi.")
			err := awaitTerminalError(t, stream.Events())
			assertProviderError(t, err, test.code, test.retryable)
		})
	}
}

func TestCloseWaitsForTheCommittedUtterance(t *testing.T) {
	t.Parallel()
	closed := make(chan websocket.StatusCode, 1)
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		expectFrames(ctx, t, conn, map[string]any{"type": "sendText", "text": "Hi."}, map[string]any{"type": "sendText", "text": "", "flush": true})
		time.Sleep(100 * time.Millisecond)
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_1"})
		_ = writeJSON(ctx, conn, audioFrame("resp_1", []byte{1, 0}))
		_ = writeJSON(ctx, conn, responseDone("resp_1", "completed", 0.1))
		_, _, err := conn.Read(ctx)
		closed <- websocket.CloseStatus(err)
	})
	defer server.Close()

	stream := openStream(t, server.URL, 24_000)
	mustSay(t, stream, "Hi.")
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	var types []protocol.EventType
	for event := range stream.Events() {
		if event.Err != nil {
			t.Fatalf("close ended with %v", event.Err)
		}
		types = append(types, event.Type)
	}
	want := []protocol.EventType{protocol.EventUsageObserved, protocol.EventAudioStarted, protocol.EventAudioFrame, protocol.EventAudioDone}
	if !reflect.DeepEqual(types, want) {
		t.Fatalf("events = %v, want %v", types, want)
	}
	if status := <-closed; status != websocket.StatusNormalClosure {
		t.Fatalf("server saw close %d, want a normal close", status)
	}
}

// TestCloseCancelsUncommittedSpeech: segmentation can speak text that was
// never committed, so Close cancels it and the started audio still gets its
// audio.done.
func TestCloseCancelsUncommittedSpeech(t *testing.T) {
	t.Parallel()
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		expectFrames(ctx, t, conn, map[string]any{"type": "sendText", "text": "First sentence. Second"})
		_ = writeJSON(ctx, conn, map[string]any{"type": "responseCreated", "response_id": "resp_1"})
		_ = writeJSON(ctx, conn, audioFrame("resp_1", []byte{1, 0}))
		expectFrames(ctx, t, conn, map[string]any{"type": "cancelResponse"})
		_ = writeJSON(ctx, conn, responseDone("resp_1", "cancelled", 0.08))
		_ = writeJSON(ctx, conn, map[string]any{"type": "cancelled"})
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	stream := openStream(t, server.URL, 24_000)
	events := stream.Events()
	waitEvent(t, events)
	if err := stream.AppendText(context.Background(), "First sentence. Second"); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, events, protocol.EventAudioStarted, protocol.EventAudioFrame)
	if err := stream.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if done := waitEvent(t, events); done.Type != protocol.EventAudioDone || !decodeBool(t, done.Data, "cancelled") {
		t.Fatalf("event = %s %s, want a cancelled audio.done", done.Type, done.Data)
	}
}

// TestCloseReportsAStall: a committed utterance that never finishes must end
// the session with an error, not a clean close.
func TestCloseReportsAStall(t *testing.T) {
	t.Parallel()
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		_, _, _ = conn.Read(ctx)
		_, _, _ = conn.Read(ctx)
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	config := ttsConfig(server.URL)
	config.GracefulCloseIdleTimeout = 100 * time.Millisecond
	adapter, err := NewTTS(config)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL, 24_000))
	if err != nil {
		t.Fatal(err)
	}
	mustSay(t, stream, "Hi.")
	assertProviderError(t, stream.Close(context.Background()), "provider_unavailable", true)
	for event := range stream.Events() {
		if event.Err != nil {
			t.Fatalf("unexpected event error %v", event.Err)
		}
	}
	assertProviderError(t, stream.(runtimepkg.TerminalErrorProviderStream).TerminalError(), "provider_unavailable", true)
}

func TestClassifiesKalpaErrors(t *testing.T) {
	t.Parallel()
	frames := map[string]string{
		"authentication_error": "authentication_failed", "account_suspended": "authentication_failed",
		"allowance_exhausted": "provider_quota_exceeded", "not_found": "invalid_request", "invalid_request": "invalid_request",
		"context_length_exceeded": "input_too_large", "rate_limit_exceeded": "provider_rate_limited",
		"service_unavailable": "provider_unavailable", "overloaded": "provider_unavailable", "inference_error": "provider_unavailable",
		"internal_error": "provider_unavailable",
	}
	for kind, want := range frames {
		message := inbound{Type: "error"}
		message.Error = &struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		}{Type: kind}
		err := frameError(message, []byte(`{}`))
		if err.Code != want || err.Retryable != (want == "provider_unavailable" || want == "provider_rate_limited") {
			t.Errorf("frame %s = %s retryable=%v, want %s", kind, err.Code, err.Retryable, want)
		}
	}
	closes := map[websocket.StatusCode]string{
		4400: "invalid_request", 4401: "authentication_failed", 4402: "provider_quota_exceeded", 4403: "authentication_failed",
		4404: "invalid_request", 4408: "request_timeout", 4429: "provider_rate_limited", 4503: "provider_unavailable", 1011: "provider_unavailable",
	}
	for status, want := range closes {
		err := closeError(status, nil)
		retryable := want == "request_timeout" || want == "provider_rate_limited" || want == "provider_unavailable"
		if err.Code != want || err.Retryable != retryable {
			t.Errorf("close %d = %s retryable=%v, want %s", status, err.Code, err.Retryable, want)
		}
	}
	for status, want := range map[int]string{0: "provider_unavailable", 401: "invalid_request", 404: "invalid_request", 429: "provider_rate_limited", 502: "provider_unavailable"} {
		if code, retryable := statusError(status); code != want || retryable != (want != "invalid_request") {
			t.Errorf("status %d = %s retryable=%v, want %s", status, code, retryable, want)
		}
	}
}

func TestKeepAliveSendsEmptyTextWhenQuiet(t *testing.T) {
	t.Parallel()
	frames := make(chan map[string]any, 1)
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		frame, _ := readFrame(ctx, conn)
		frames <- frame
		_, _, _ = conn.Read(ctx)
	})
	defer server.Close()

	config := ttsConfig(server.URL)
	config.KeepAliveInterval = 50 * time.Millisecond
	adapter, err := NewTTS(config)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL, 24_000))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(*ttsStream).abort() })
	select {
	case frame := <-frames:
		if want := map[string]any{"type": "sendText", "text": ""}; !reflect.DeepEqual(frame, want) {
			t.Fatalf("keepalive = %#v, want %#v", frame, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no keepalive on a quiet socket")
	}
}

// TestStalledKeepAliveDoesNotBlockCommands: a command issued while a
// keepalive is parked on a socket that stopped accepting writes must honour
// its own context instead of waiting behind the keepalive.
func TestStalledKeepAliveDoesNotBlockCommands(t *testing.T) {
	t.Parallel()
	stream, socket := stalledSession(t, 20*time.Millisecond)
	socket.stalled.Store(true)
	select {
	case <-socket.parked:
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive never wrote")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- stream.AppendText(ctx, "hello") }()
	select {
	case err := <-done:
		var providerError *runtimepkg.ProviderError
		if !errors.As(err, &providerError) || !errors.Is(providerError.Cause, context.DeadlineExceeded) {
			t.Fatalf("AppendText error = %v, want its own deadline", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AppendText blocked behind the stalled keepalive")
	}
}

// TestCloseOnAStalledSocket: the runtime closes with no deadline, so Close's
// cancel of uncommitted text must not park on a socket that stopped
// accepting writes.
func TestCloseOnAStalledSocket(t *testing.T) {
	t.Parallel()
	stream, socket := stalledSession(t, time.Hour)
	if err := stream.AppendText(context.Background(), "uncommitted"); err != nil {
		t.Fatal(err)
	}
	socket.stalled.Store(true)
	done := make(chan error, 1)
	go func() { done <- stream.Close(context.Background()) }()
	select {
	case err := <-done:
		assertProviderError(t, err, "provider_unavailable", true)
	case <-time.After(3 * time.Second):
		t.Fatal("Close parked on the stalled socket")
	}
}

// stallConn is the client's side of the socket. While stalled, every Write
// parks until Close, like a connection whose peer stopped reading.
type stallConn struct {
	net.Conn
	stalled   atomic.Bool
	parked    chan struct{}
	parkOnce  sync.Once
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *stallConn) Write(p []byte) (int, error) {
	if c.stalled.Load() {
		c.parkOnce.Do(func() { close(c.parked) })
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}

func (c *stallConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// stalledSession opens a stream whose socket can be made to stop accepting
// writes, against a server that keeps reading.
func stalledSession(t *testing.T, keepAlive time.Duration) (runtimepkg.ProviderStream, *stallConn) {
	t.Helper()
	server := newSocketServer(t, func(ctx context.Context, conn *websocket.Conn) {
		handshakeOK(ctx, t, conn, 24_000)
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	})
	t.Cleanup(server.Close)
	socket := &stallConn{parked: make(chan struct{}), closed: make(chan struct{})}
	config := ttsConfig(server.URL)
	config.HTTPClient = &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		socket.Conn = conn
		return socket, err
	}}}
	config.KeepAliveInterval = keepAlive
	config.GracefulCloseIdleTimeout = 200 * time.Millisecond
	adapter, err := NewTTS(config)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(server.URL, 24_000))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(*ttsStream).abort() })
	return stream, socket
}

func openStream(t *testing.T, serverURL string, rate int) runtimepkg.ProviderStream {
	t.Helper()
	adapter, err := NewTTS(ttsConfig(serverURL))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.Open(context.Background(), ttsRequest(serverURL, rate))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.(*ttsStream).abort() })
	return stream
}

func mustSay(t *testing.T, stream runtimepkg.ProviderStream, text string) {
	t.Helper()
	if err := stream.AppendText(context.Background(), text); err != nil {
		t.Fatal(err)
	}
	if err := stream.CommitText(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func handshakeOK(ctx context.Context, t *testing.T, conn *websocket.Conn, rate int) {
	t.Helper()
	if _, err := readFrame(ctx, conn); err != nil {
		t.Errorf("read init: %v", err)
	}
	_ = writeJSON(ctx, conn, sessionCreated(rate))
}

func expectFrames(ctx context.Context, t *testing.T, conn *websocket.Conn, want ...map[string]any) {
	t.Helper()
	for _, frame := range want {
		got, err := readFrame(ctx, conn)
		if err != nil || !reflect.DeepEqual(got, frame) {
			t.Errorf("frame = %#v (%v), want %#v", got, err, frame)
		}
	}
}

func expectEvents(t *testing.T, events <-chan runtimepkg.ProviderEvent, want ...protocol.EventType) {
	t.Helper()
	for _, eventType := range want {
		if event := waitEvent(t, events); event.Type != eventType {
			t.Fatalf("event = %s %s, want %s", event.Type, event.Data, eventType)
		}
	}
}

func sessionCreated(rate int) map[string]any {
	return map[string]any{
		"type": "sessionCreated", "session_id": "sess_1", "model": "kalpa-tts-1", "voice_id": testVoice,
		"output_format": "pcm_s16le", "sample_rate": rate, "channels": 1, "audio_quality": "high",
	}
}

func audioFrame(responseID string, pcm []byte) map[string]any {
	return map[string]any{"type": "responseAudio", "response_id": responseID, "pcm_b64": base64.StdEncoding.EncodeToString(pcm)}
}

func responseDone(responseID, status string, seconds float64) map[string]any {
	return map[string]any{
		"type": "responseDone", "response_id": responseID, "status": status, "text": "",
		"usage": map[string]any{"input_chars": 0, "input_audio_seconds": 0, "output_audio_seconds": seconds},
	}
}

func assertProviderError(t *testing.T, err error, code string, retryable bool) {
	t.Helper()
	var providerError *runtimepkg.ProviderError
	if !errors.As(err, &providerError) {
		t.Fatalf("error = %v, want a ProviderError %s", err, code)
	}
	if providerError.Code != code || providerError.Retryable != retryable {
		t.Fatalf("error = %s retryable=%v (%v), want %s retryable=%v", providerError.Code, providerError.Retryable, err, code, retryable)
	}
}

func newSocketServer(t *testing.T, callback func(context.Context, *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tts/"+testVoice+"/stream" {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		go func() {
			defer cancel()
			defer conn.CloseNow()
			callback(ctx, conn)
		}()
	}))
}

// readFrame decodes into an untyped map on purpose. Unmarshalling into the
// adapter's own structs would make a misspelled json tag invisible.
func readFrame(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText {
		return nil, fmt.Errorf("frame type = %v, want text", messageType)
	}
	frame := map[string]any{}
	if err := json.Unmarshal(payload, &frame); err != nil {
		return nil, fmt.Errorf("decode %q: %w", payload, err)
	}
	return frame, nil
}

func waitEvent(t *testing.T, events <-chan runtimepkg.ProviderEvent) runtimepkg.ProviderEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("provider events closed early")
		}
		if event.Err != nil {
			t.Fatalf("provider event error: %v", event.Err)
		}
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a provider event")
		return runtimepkg.ProviderEvent{}
	}
}

func awaitTerminalError(t *testing.T, events <-chan runtimepkg.ProviderEvent) error {
	t.Helper()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("provider events closed without a terminal error")
			}
			if event.Err != nil {
				return event.Err
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for a terminal error")
			return nil
		}
	}
}

func decodeString(t *testing.T, data json.RawMessage, field string) string {
	t.Helper()
	decoded := map[string]any{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode event data %q: %v", data, err)
	}
	value, _ := decoded[field].(string)
	return value
}

func decodeBool(t *testing.T, data json.RawMessage, field string) bool {
	t.Helper()
	decoded := map[string]any{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode event data %q: %v", data, err)
	}
	value, _ := decoded[field].(bool)
	return value
}

func ttsConfig(serverURL string) TTSConfig {
	endpoint, _ := url.Parse(serverURL)
	return TTSConfig{AllowedEndpointHosts: []string{endpoint.Hostname()}, AllowInsecureEndpoint: true}
}

func ttsRequest(serverURL string, rate int) runtimepkg.AdapterRequest {
	now := time.Now().UTC()
	return runtimepkg.AdapterRequest{
		Kind: protocol.SessionKindTTS,
		Plan: protocol.SessionPlan{
			PlanID: "plan_kalpa", SessionID: "sess_kalpa", AttemptID: "att_1",
			Execution: protocol.Execution{
				Placement: protocol.PlacementEmbedded, ProviderRoute: protocol.RouteProviderDirect, CredentialSource: protocol.CredentialsBYOK,
			},
			ExpiresAt: now.Add(time.Hour),
			Route: protocol.PlanRoute{
				Provider: "kalpa", Model: "kalpa-tts-1", Adapter: TTSAdapterID, Voice: testVoice,
				Transport: protocol.TransportWebSocket, Endpoint: endpointFor(serverURL, "/v1/tts"),
				Credential: &protocol.DelegatedCredential{Kind: protocol.CredentialBearer, Value: testKey, ExpiresAt: now.Add(30 * time.Minute)},
			},
		},
		Options: protocol.RequestOptions{Language: "en"},
		Media:   &protocol.MediaFormat{Encoding: "pcm_s16le", SampleRateHz: rate, Channels: 1},
	}
}

func endpointFor(serverURL, path string) string {
	endpoint, _ := url.Parse(serverURL)
	endpoint.Scheme = "ws"
	endpoint.Path = path
	return endpoint.String()
}
