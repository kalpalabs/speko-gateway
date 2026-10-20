package kalpa

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/SpekoAI/gateway/internal/upstream"
	"github.com/SpekoAI/gateway/protocol"
	runtimepkg "github.com/SpekoAI/gateway/runtime"
	"github.com/coder/websocket"
)

const (
	// TTSAdapterID is the identifier returned by a Kalpa TTS session plan.
	TTSAdapterID = "kalpa.tts.v1"

	providerName    = "kalpa"
	officialAPIHost = "api.kalpalabs.ai"
	ttsBasePath     = "/v1/tts"
	extensionID     = "kalpalabs.ai/v1"

	// maxUtteranceChars is Kalpa's limit on one utterance's text. The adapter
	// counts code points across every sendText of the utterance.
	maxUtteranceChars = 8_000

	defaultHandshakeTimeout  = 10 * time.Second
	defaultKeepAliveInterval = 60 * time.Second
	defaultCloseIdleTimeout  = 30 * time.Second
)

var (
	// sampleRates are the output rates Kalpa streams.
	sampleRates = map[int]struct{}{
		8_000: {}, 16_000: {}, 22_050: {}, 24_000: {}, 32_000: {}, 44_100: {}, 48_000: {},
	}
	languages = map[string]struct{}{"en": {}, "hi": {}}
)

// TTSConfig controls local transport limits. Credentials, model, and voice
// always come from the signed session plan and its request options.
type TTSConfig struct {
	AdapterID       string
	HTTPClient      *http.Client
	EventBuffer     int
	MaxMessageBytes int64
	// HandshakeTimeout bounds the wait for sessionCreated after
	// initializeConnection.
	HandshakeTimeout time.Duration
	// KeepAliveInterval is how long the socket may carry no client message
	// before an empty sendText keeps it open. Kalpa closes a socket that
	// receives no client message for several minutes; WebSocket pings do not
	// count.
	KeepAliveInterval time.Duration
	// GracefulCloseIdleTimeout bounds how long Close waits without progress
	// for a committed utterance to finish.
	GracefulCloseIdleTimeout time.Duration
	AllowedEndpointHosts     []string
	AllowInsecureEndpoint    bool
}

// TTSAdapter implements Kalpa's WSS /v1/tts/{voice_id}/stream API.
type TTSAdapter struct {
	id               string
	httpClient       *http.Client
	eventBuffer      int
	maxMessageBytes  int64
	handshakeTimeout time.Duration
	keepAlive        time.Duration
	closeIdleTimeout time.Duration
	endpointPolicy   upstream.WebSocketPolicy
}

// NewTTS creates a bounded Kalpa TTS adapter.
func NewTTS(config TTSConfig) (*TTSAdapter, error) {
	if config.AdapterID == "" {
		config.AdapterID = TTSAdapterID
	}
	if config.EventBuffer == 0 {
		config.EventBuffer = 32
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = defaultHandshakeTimeout
	}
	if config.KeepAliveInterval == 0 {
		config.KeepAliveInterval = defaultKeepAliveInterval
	}
	if config.GracefulCloseIdleTimeout == 0 {
		config.GracefulCloseIdleTimeout = defaultCloseIdleTimeout
	}
	if config.EventBuffer < 1 || config.MaxMessageBytes < 1 {
		return nil, errors.New("kalpa tts buffers must be positive")
	}
	if config.HandshakeTimeout < 0 || config.KeepAliveInterval < 0 || config.GracefulCloseIdleTimeout < 0 {
		return nil, errors.New("kalpa tts timeouts must be positive")
	}
	policy, err := upstream.NewWebSocketPolicy(officialAPIHost, config.AllowedEndpointHosts, config.AllowInsecureEndpoint)
	if err != nil {
		return nil, err
	}
	return &TTSAdapter{
		id: config.AdapterID, httpClient: config.HTTPClient, eventBuffer: config.EventBuffer,
		maxMessageBytes: config.MaxMessageBytes, handshakeTimeout: config.HandshakeTimeout,
		keepAlive: config.KeepAliveInterval, closeIdleTimeout: config.GracefulCloseIdleTimeout,
		endpointPolicy: policy,
	}, nil
}

func (a *TTSAdapter) ID() string { return a.id }

// Open dials the voice's socket and completes the initializeConnection →
// sessionCreated handshake before returning, so a refused key, voice, model
// or sample rate fails the open rather than the first utterance.
func (a *TTSAdapter) Open(ctx context.Context, request runtimepkg.AdapterRequest) (runtimepkg.ProviderStream, error) {
	if request.Kind != protocol.SessionKindTTS {
		return nil, fmt.Errorf("kalpa supports tts sessions, got %q", request.Kind)
	}
	if request.Plan.Route.Provider != providerName {
		return nil, fmt.Errorf("kalpa adapter cannot open provider %q", request.Plan.Route.Provider)
	}
	if request.Plan.Route.Transport != protocol.TransportWebSocket {
		return nil, fmt.Errorf("kalpa tts requires websocket transport, got %q", request.Plan.Route.Transport)
	}
	if request.Plan.Execution.CredentialSource == protocol.CredentialsManaged && request.Plan.Execution.ProviderRoute != protocol.RouteSpekoRelay {
		return nil, errors.New("kalpa managed provider-direct routing is unavailable because Kalpa does not issue short-lived credentials")
	}
	model := strings.TrimSpace(request.Plan.Route.Model)
	if model == "" || model == "auto" {
		return nil, errors.New("kalpa requires a concrete model in the session plan")
	}
	if request.Media == nil {
		return nil, errors.New("kalpa tts requires media configuration")
	}
	if err := checkMedia(*request.Media); err != nil {
		return nil, err
	}
	if err := checkLanguage(request.Options.Language); err != nil {
		return nil, err
	}
	voice := strings.TrimSpace(request.Options.Voice)
	if voice == "" {
		voice = strings.TrimSpace(request.Plan.Route.Voice)
	}
	credential := request.Plan.Route.Credential
	if credential == nil || !acceptableCredentialKind(request.Plan.Execution.ProviderRoute, credential.Kind) || strings.TrimSpace(credential.Value) == "" {
		return nil, errors.New("kalpa tts requires a bearer credential")
	}
	endpoint, err := voiceEndpoint(a.endpointPolicy, request.Plan.Route.Endpoint, voice)
	if err != nil {
		return nil, err
	}

	client := a.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: client})
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		code, retryable := statusError(status)
		return nil, &runtimepkg.ProviderError{Code: code, Message: "Kalpa TTS connection could not be established", Retryable: retryable, ProviderStatus: status, Cause: err}
	}
	conn.SetReadLimit(a.maxMessageBytes)
	created, raw, err := handshake(ctx, conn, a.handshakeTimeout, initializeConnection{
		Type: "initializeConnection", APIKey: credential.Value, Model: model,
		OutputFormat: "pcm_s16le", SampleRate: request.Media.SampleRateHz,
		GenerationConfig: &struct{}{},
	})
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	stream := &ttsStream{
		conn: conn, ctx: streamCtx, cancel: cancel, events: make(chan runtimepkg.ProviderEvent, a.eventBuffer),
		model: created.Model, sessionID: created.SessionID, closeIdleTimeout: a.closeIdleTimeout,
		progress: make(chan struct{}, 1), readDone: make(chan struct{}), lastWrite: time.Now(),
	}
	stream.events <- runtimepkg.ProviderEvent{Type: protocol.EventUsageObserved, Data: requestData(created.SessionID), Extensions: extension(raw)}
	go stream.readLoop()
	go stream.keepAlive(a.keepAlive)
	return stream, nil
}

// handshake sends initializeConnection and waits for sessionCreated. Every
// refusal is a fatal error frame followed by a close; the frame names the
// cause, and a close without one (an init deadline, a malformed frame) is
// classified by its code.
func handshake(ctx context.Context, conn *websocket.Conn, timeout time.Duration, init initializeConnection) (inbound, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := writeJSON(ctx, conn, init); err != nil {
		return inbound{}, nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS initialization could not be sent", Retryable: true, Cause: err}
	}
	for {
		messageType, payload, err := conn.Read(ctx)
		if err != nil {
			if status := websocket.CloseStatus(err); status != -1 {
				return inbound{}, nil, closeError(status, err)
			}
			if ctx.Err() != nil {
				return inbound{}, nil, &runtimepkg.ProviderError{Code: "request_timeout", Message: "Kalpa TTS did not confirm the session in time", Retryable: true, Cause: err}
			}
			return inbound{}, nil, &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS initialization failed", Retryable: true, Cause: err}
		}
		if messageType != websocket.MessageText {
			continue
		}
		var message inbound
		if err := json.Unmarshal(payload, &message); err != nil {
			return inbound{}, nil, malformed(err)
		}
		switch message.Type {
		case "error":
			return inbound{}, nil, frameError(message, payload)
		case "sessionCreated":
			if message.OutputFormat != "pcm_s16le" || message.SampleRate != init.SampleRate || message.Channels != 1 {
				return inbound{}, nil, &runtimepkg.ProviderError{
					Code:       "unsupported_media",
					Message:    fmt.Sprintf("Kalpa TTS confirmed %s at %d Hz with %d channels; the session asked for mono pcm_s16le at %d Hz", message.OutputFormat, message.SampleRate, message.Channels, init.SampleRate),
					Retryable:  false,
					Extensions: extension(payload),
				}
			}
			return message, payload, nil
		}
	}
}

// utterance is one text.append…text.commit unit, which Kalpa answers with one
// response.
type utterance struct {
	chars        int
	committed    bool
	responseID   string
	audioStarted bool
	// billing holds a completed response's observation when a Cancel raced
	// its responseDone, so the cancelled audio.done still carries it.
	billing *protocol.BillingObservation
	done    chan struct{}
}

type ttsStream struct {
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	events chan runtimepkg.ProviderEvent

	model            string
	sessionID        string
	closeIdleTimeout time.Duration
	progress         chan struct{}
	readDone         chan struct{}

	writeMu   sync.Mutex
	lastWrite time.Time
	closeOnce sync.Once
	abortOnce sync.Once
	closed    atomic.Bool
	closing   atomic.Bool

	stateMu  sync.Mutex
	closeErr error
	// current is the caller's open or committed utterance; nil between
	// utterances.
	current *utterance
	// cancelled are utterances stopped by Cancel, oldest first, each awaiting
	// its `cancelled` acknowledgement.
	cancelled []*utterance
}

func (s *ttsStream) Events() <-chan runtimepkg.ProviderEvent { return s.events }
func (s *ttsStream) WriteAudio(context.Context, []byte) error {
	return runtimepkg.ErrUnsupportedOperation
}
func (s *ttsStream) CommitAudio(context.Context) error { return runtimepkg.ErrUnsupportedOperation }

// AppendText streams one chunk into the current utterance verbatim; the
// caller owns spacing between chunks. With generation_config set at init,
// Kalpa starts rendering complete sentences before the commit.
func (s *ttsStream) AppendText(ctx context.Context, text string) error {
	if text == "" {
		return errors.New("kalpa text is empty")
	}
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	if s.current == nil {
		s.current = &utterance{done: make(chan struct{})}
	} else if s.current.committed {
		s.stateMu.Unlock()
		return errors.New("kalpa previous utterance has not completed")
	}
	chars := s.current.chars + utf8.RuneCountInString(text)
	if chars > maxUtteranceChars {
		s.stateMu.Unlock()
		return &runtimepkg.ProviderError{
			Code:      "input_too_large",
			Message:   fmt.Sprintf("Kalpa TTS utterances are limited to %d characters", maxUtteranceChars),
			Hint:      "Commit long text as several shorter utterances.",
			Retryable: false,
		}
	}
	s.current.chars = chars
	s.stateMu.Unlock()
	return s.writeJSON(ctx, sendText{Type: "sendText", Text: text})
}

// CommitText flushes the current utterance. A commit with no text is valid:
// Kalpa answers it with a completed response that has no audio.
func (s *ttsStream) CommitText(ctx context.Context) error {
	s.stateMu.Lock()
	if s.closed.Load() || s.closing.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	if s.current == nil {
		s.current = &utterance{done: make(chan struct{})}
	} else if s.current.committed {
		s.stateMu.Unlock()
		return errors.New("kalpa previous utterance has not completed")
	}
	s.current.committed = true
	s.stateMu.Unlock()
	return s.writeJSON(ctx, sendText{Type: "sendText", Flush: true})
}

// Cancel stops the current utterance, committed or not, and leaves the
// socket ready for the next one at once. A bare cancelResponse abandons the
// in-flight response and any buffered text; Kalpa answers it with exactly one
// `cancelled`, after the last message of the work it stopped and before any
// later response starts. Until then every response message is that stopped
// work and is dropped. Cancel with nothing in flight is a no-op.
func (s *ttsStream) Cancel(ctx context.Context) error {
	s.stateMu.Lock()
	if s.closed.Load() {
		s.stateMu.Unlock()
		return runtimepkg.ErrSessionClosed
	}
	stopped := s.current
	if stopped == nil {
		s.stateMu.Unlock()
		return nil
	}
	s.current = nil
	s.cancelled = append(s.cancelled, stopped)
	s.stateMu.Unlock()
	return s.writeJSON(ctx, map[string]string{"type": "cancelResponse"})
}

// Close lets a committed utterance and any pending cancellations finish, then
// closes the socket normally. Uncommitted text is never spoken: Close cancels
// it, so an utterance whose audio already started still ends with audio.done.
func (s *ttsStream) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		s.stateMu.Lock()
		uncommitted := s.current != nil && !s.current.committed
		s.stateMu.Unlock()
		var err error
		if uncommitted {
			err = s.Cancel(ctx)
		}
		if err == nil {
			err = s.drain(ctx)
		}
		if err == nil {
			s.closed.Store(true)
			select {
			case <-s.readDone:
				// The server already ended the socket; the read loop reported why.
			default:
				if closeErr := s.conn.Close(websocket.StatusNormalClosure, ""); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
					err = closeErr
				}
			}
		}
		if err != nil {
			s.stateMu.Lock()
			s.closeErr = err
			s.stateMu.Unlock()
			_ = s.abort()
		}
	})
	return s.TerminalError()
}

// TerminalError reports a failed graceful close. The read loop cannot report
// it, because Close has already marked the socket closed when it gives up.
func (s *ttsStream) TerminalError() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.closeErr
}

// Abort immediately tears down the socket after a terminal runtime failure.
func (s *ttsStream) Abort(context.Context) error { return s.abort() }

func (s *ttsStream) abort() error {
	s.abortOnce.Do(func() {
		s.closed.Store(true)
		s.cancel()
		_ = s.conn.CloseNow()
	})
	return nil
}

func (s *ttsStream) drain(ctx context.Context) error {
	s.stateMu.Lock()
	var pending []chan struct{}
	for _, stopped := range s.cancelled {
		pending = append(pending, stopped.done)
	}
	if s.current != nil && s.current.committed {
		pending = append(pending, s.current.done)
	}
	s.stateMu.Unlock()

	timer := time.NewTimer(s.closeIdleTimeout)
	defer timer.Stop()
	for _, done := range pending {
	wait:
		for {
			select {
			case <-done:
				break wait
			case <-s.readDone:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-s.progress:
				timer.Reset(s.closeIdleTimeout)
			case <-timer.C:
				return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS stalled during graceful close", Retryable: true, Cause: context.DeadlineExceeded}
			}
		}
	}
	return nil
}

// keepAlive sends an empty sendText, which changes nothing on the server,
// whenever the caller has been quiet for a whole interval. Gateway sessions
// can sit between utterances longer than Kalpa keeps an idle socket open.
func (s *ttsStream) keepAlive(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.writeMu.Lock()
			idle := time.Since(s.lastWrite) >= interval
			s.writeMu.Unlock()
			if idle && s.writeJSON(s.ctx, sendText{Type: "sendText"}) != nil {
				return
			}
		}
	}
}

func (s *ttsStream) writeJSON(ctx context.Context, value any) error {
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.closed.Load() {
		return runtimepkg.ErrSessionClosed
	}
	if err := writeJSON(ctx, s.conn, value); err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS write failed", Retryable: true, Cause: err}
	}
	s.lastWrite = time.Now()
	return nil
}

func (s *ttsStream) readLoop() {
	defer func() {
		s.cancel()
		close(s.events)
		close(s.readDone)
	}()
	for {
		messageType, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			if !s.closed.Load() && s.ctx.Err() == nil {
				if cause := s.endError(err); cause != nil {
					_ = s.emit(runtimepkg.ProviderEvent{Err: cause})
				}
			}
			return
		}
		s.reportProgress()
		if messageType != websocket.MessageText {
			continue
		}
		if err := s.handleMessage(payload); err != nil {
			_ = s.emit(runtimepkg.ProviderEvent{Err: err})
			return
		}
	}
}

// endError classifies a socket the server ended. A normal close with nothing
// outstanding is a clean end.
func (s *ttsStream) endError(err error) error {
	status := websocket.CloseStatus(err)
	if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		s.stateMu.Lock()
		idle := s.current == nil && len(s.cancelled) == 0
		s.stateMu.Unlock()
		if idle {
			return nil
		}
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS closed the socket mid-utterance", Retryable: true, Cause: err}
	}
	if status != -1 {
		return closeError(status, err)
	}
	return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS read failed", Retryable: true, Cause: err}
}

func (s *ttsStream) handleMessage(payload []byte) error {
	var message inbound
	if err := json.Unmarshal(payload, &message); err != nil {
		return malformed(err)
	}
	switch message.Type {
	case "responseCreated":
		s.stateMu.Lock()
		if len(s.cancelled) > 0 {
			if stopped := s.cancelled[0]; stopped.responseID == "" {
				stopped.responseID = message.ResponseID
			}
		} else if s.current != nil && s.current.responseID == "" {
			s.current.responseID = message.ResponseID
		}
		s.stateMu.Unlock()
		return nil
	case "responseAudio":
		return s.handleAudio(message)
	case "responseDone":
		return s.handleDone(message, payload)
	case "cancelled":
		s.stateMu.Lock()
		if len(s.cancelled) == 0 {
			s.stateMu.Unlock()
			return nil
		}
		stopped := s.cancelled[0]
		s.cancelled = s.cancelled[1:]
		s.stateMu.Unlock()
		err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: doneData(stopped.responseID, true), Extensions: extension(payload), Billing: stopped.billing})
		close(stopped.done)
		return err
	case "error":
		// A response the server failed on its own settles before the ack of a
		// cancel that already stopped it; only that work can be reporting.
		s.stateMu.Lock()
		stale := !message.Fatal && message.ResponseID != "" && len(s.cancelled) > 0
		s.stateMu.Unlock()
		if stale {
			return nil
		}
		return frameError(message, payload)
	default:
		return nil
	}
}

func (s *ttsStream) handleAudio(message inbound) error {
	s.stateMu.Lock()
	active := s.current
	if len(s.cancelled) > 0 || active == nil || active.responseID != message.ResponseID {
		s.stateMu.Unlock()
		return nil
	}
	s.stateMu.Unlock()

	audio, err := base64.StdEncoding.DecodeString(message.PCM)
	if err != nil {
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS sent invalid audio data", Retryable: true, Cause: err}
	}
	if len(audio) == 0 {
		return nil
	}
	s.stateMu.Lock()
	started := !active.audioStarted
	active.audioStarted = true
	s.stateMu.Unlock()
	data := requestData(message.ResponseID)
	if started {
		if err := s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioStarted, Data: data}); err != nil {
			return err
		}
	}
	return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioFrame, Data: data, Audio: audio})
}

func (s *ttsStream) handleDone(message inbound, payload []byte) error {
	completed := message.Status == "completed"
	s.stateMu.Lock()
	if len(s.cancelled) > 0 {
		// The stopped work settling. If it completed before the cancel reached
		// the server, Kalpa bills it, so keep the evidence for the ack.
		if stopped := s.cancelled[0]; completed && stopped.responseID == message.ResponseID {
			stopped.billing = s.billing(message)
		}
		s.stateMu.Unlock()
		return nil
	}
	finished := s.current
	if finished == nil || !finished.committed || finished.responseID != message.ResponseID {
		s.stateMu.Unlock()
		return nil
	}
	s.current = nil
	s.stateMu.Unlock()
	defer close(finished.done)
	if !completed {
		// Kalpa cancels a response on its own only after an error frame, which
		// has already ended the session.
		return &runtimepkg.ProviderError{Code: "provider_unavailable", Message: "Kalpa TTS ended the response without completing it", Retryable: true, Extensions: extension(payload)}
	}
	return s.emit(runtimepkg.ProviderEvent{Type: protocol.EventAudioDone, Data: doneData(message.ResponseID, false), Extensions: extension(payload), Billing: s.billing(message)})
}

// billing is one completed response's observation. Kalpa bills completed
// responses by the duration of audio generated, which responseDone reports.
func (s *ttsStream) billing(message inbound) *protocol.BillingObservation {
	if message.Usage == nil || message.Usage.OutputAudioSeconds == nil {
		return nil
	}
	return &protocol.BillingObservation{
		ProviderRequestID: s.sessionID, ProviderResponseID: message.ResponseID, OperationID: message.ResponseID,
		Model: s.model, Mode: "streaming", Complete: true,
		Quantities: map[string]int64{"duration_seconds": int64(math.Round(*message.Usage.OutputAudioSeconds * 1000))},
	}
}

func (s *ttsStream) emit(event runtimepkg.ProviderEvent) error {
	select {
	case s.events <- event:
		s.reportProgress()
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ttsStream) reportProgress() {
	select {
	case s.progress <- struct{}{}:
	default:
	}
}

type initializeConnection struct {
	Type             string    `json:"type"`
	APIKey           string    `json:"api_key"`
	Model            string    `json:"model"`
	OutputFormat     string    `json:"output_format"`
	SampleRate       int       `json:"sample_rate"`
	GenerationConfig *struct{} `json:"generation_config"`
}

type sendText struct {
	Type  string `json:"type"`
	Text  string `json:"text"`
	Flush bool   `json:"flush,omitempty"`
}

// inbound covers every server message on the socket.
type inbound struct {
	Type         string `json:"type"`
	SessionID    string `json:"session_id"`
	Model        string `json:"model"`
	OutputFormat string `json:"output_format"`
	SampleRate   int    `json:"sample_rate"`
	Channels     int    `json:"channels"`
	ResponseID   string `json:"response_id"`
	PCM          string `json:"pcm_b64"`
	Status       string `json:"status"`
	Usage        *struct {
		OutputAudioSeconds *float64 `json:"output_audio_seconds"`
	} `json:"usage"`
	Fatal bool `json:"fatal"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func requestData(id string) json.RawMessage {
	return marshalData(map[string]any{"provider_request_id": id})
}

// doneData omits the response id of a stopped utterance Kalpa never started.
func doneData(responseID string, cancelled bool) json.RawMessage {
	data := map[string]any{"cancelled": cancelled}
	if responseID != "" {
		data["provider_request_id"] = responseID
	}
	return marshalData(data)
}
