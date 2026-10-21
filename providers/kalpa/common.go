package kalpa

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

func acceptableCredentialKind(route protocol.ProviderRoute, kind protocol.CredentialKind) bool {
	return kind == protocol.CredentialBearer || (route == protocol.RouteSpekoRelay && kind == protocol.CredentialRelayAccess)
}

// voiceEndpoint appends the voice to the plan's /v1/tts base. Voice ids are
// UUIDs; anything that is not a single path segment is refused rather than
// escaped into a path nobody intended.
func voiceEndpoint(policy upstream.WebSocketPolicy, rawEndpoint, voice string) (string, error) {
	endpoint, err := policy.Parse(rawEndpoint)
	if err != nil {
		return "", fmt.Errorf("kalpa endpoint: %w", err)
	}
	if strings.TrimRight(endpoint.Path, "/") != ttsBasePath {
		return "", fmt.Errorf("kalpa endpoint path must be %s, got %q", ttsBasePath, endpoint.Path)
	}
	if voice == "" {
		return "", errors.New("kalpa requires a voice id in request options or the session plan")
	}
	for _, r := range voice {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return "", &runtimepkg.ProviderError{Code: "invalid_request", Message: fmt.Sprintf("Kalpa voice id %q is not valid", voice), Hint: "Use a voice id from Kalpa's GET /v1/voices.", Retryable: false}
		}
	}
	endpoint.Path = ttsBasePath + "/" + voice + "/stream"
	return endpoint.String(), nil
}

// checkMedia refuses formats Kalpa cannot produce before anything is dialled.
func checkMedia(media protocol.MediaFormat) error {
	if err := media.Validate(); err != nil {
		return &runtimepkg.ProviderError{Code: "unsupported_media", Message: fmt.Sprintf("kalpa tts media: %v", err), Retryable: false}
	}
	if media.Encoding != "pcm_s16le" || media.Channels != 1 {
		return &runtimepkg.ProviderError{Code: "unsupported_media", Message: "Kalpa TTS streams mono pcm_s16le only", Retryable: false}
	}
	if _, ok := sampleRates[media.SampleRateHz]; !ok {
		return &runtimepkg.ProviderError{
			Code:      "unsupported_media",
			Message:   fmt.Sprintf("Kalpa TTS does not support %d Hz output", media.SampleRateHz),
			Hint:      "Use 8000, 16000, 22050, 24000, 32000, 44100 or 48000 Hz.",
			Retryable: false,
		}
	}
	return nil
}

// checkLanguage gates routing only: the socket takes no language and reads it
// from the text, so a supported language is never sent upstream.
func checkLanguage(language string) error {
	tag := strings.ToLower(strings.TrimSpace(language))
	if tag == "" || tag == "auto" {
		return nil
	}
	base, _, _ := strings.Cut(strings.ReplaceAll(tag, "_", "-"), "-")
	if _, ok := languages[base]; ok {
		return nil
	}
	return &runtimepkg.ProviderError{
		Code:      "unsupported_language",
		Message:   fmt.Sprintf("Kalpa TTS does not support language %q", language),
		Hint:      "Kalpa speaks English (en) and Hindi (hi); use another TTS provider for this language.",
		Retryable: false,
	}
}

// statusError classifies a refused WebSocket upgrade. Kalpa authenticates in
// the first message, not on the upgrade, so a 401 or 403 here is not a
// verdict on the key.
func statusError(status int) (string, bool) {
	switch {
	case status == http.StatusTooManyRequests:
		return "provider_rate_limited", true
	case status >= 400 && status < 500:
		return "invalid_request", false
	default:
		return "provider_unavailable", true
	}
}

// closeError classifies a close that arrived without an error frame naming
// its cause.
func closeError(status websocket.StatusCode, cause error) *runtimepkg.ProviderError {
	code, retryable := "provider_unavailable", true
	switch status {
	case 4400, 4404:
		code, retryable = "invalid_request", false
	case 4401, 4403:
		code, retryable = "authentication_failed", false
	case 4402:
		code, retryable = "provider_quota_exceeded", false
	case 4408:
		code = "request_timeout"
	case 4429:
		code = "provider_rate_limited"
	}
	return &runtimepkg.ProviderError{Code: code, Message: fmt.Sprintf("Kalpa TTS closed the socket (%d)", status), Retryable: retryable, Cause: cause}
}

// frameError classifies an error frame by its error.type. The gateway does not
// retry inside a session, so even a response-scoped error Kalpa's socket
// survives (overloaded, inference_error) ends the session, marked retryable
// so the client can open a new one.
func frameError(message inbound, raw []byte) *runtimepkg.ProviderError {
	var kind, detail string
	if message.Error != nil {
		kind, detail = message.Error.Type, message.Error.Message
	}
	err := &runtimepkg.ProviderError{Code: "provider_unavailable", Retryable: true, Extensions: extension(raw)}
	switch kind {
	case "authentication_error":
		err.Code, err.Retryable, err.Hint = "authentication_failed", false, "Check the Kalpa API key."
	case "account_suspended":
		err.Code, err.Retryable = "authentication_failed", false
	case "allowance_exhausted":
		err.Code, err.Retryable = "provider_quota_exceeded", false
	case "not_found":
		err.Code, err.Retryable, err.Hint = "invalid_request", false, "Use a voice id from Kalpa's GET /v1/voices for this model."
	case "invalid_request":
		err.Code, err.Retryable = "invalid_request", false
	case "context_length_exceeded":
		err.Code, err.Retryable, err.Hint = "context_length_exceeded", false, "Commit shorter utterances."
	case "rate_limit_exceeded":
		err.Code = "provider_rate_limited"
	}
	err.Message = "Kalpa TTS reported " + kind
	if kind == "" {
		err.Message = "Kalpa TTS reported an error"
	}
	if detail = strings.TrimSpace(detail); detail != "" {
		err.Message += ": " + detail
	}
	return err
}

func malformed(cause error) *runtimepkg.ProviderError {
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS sent malformed JSON", Retryable: true, Cause: cause}
}

func extension(raw []byte) map[string]json.RawMessage {
	return map[string]json.RawMessage{extensionID: json.RawMessage(append([]byte(nil), raw...))}
}

func marshalData(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{"code":"internal"}`)
	}
	return data
}
