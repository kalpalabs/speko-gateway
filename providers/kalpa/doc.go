// Package kalpa implements the provider-direct Kalpa text-to-speech adapter.
//
//	tts: kalpa.tts.v1, wss://api.kalpalabs.ai/v1/tts/{voice_id}/stream
//
// One socket serves one voice, so the voice id is part of the path and the
// catalog endpoint is the /v1/tts base. The first message, initializeConnection,
// authenticates with the API key and fixes the model and output for the whole
// socket; sessionCreated confirms them. Open completes that handshake, so a bad
// key (close 4401), a used-up allowance (4402), an unknown voice (4404) or a
// refused setting fails the open, not the first utterance.
//
// # Output
//
// Kalpa streams mono pcm_s16le at the session's rate: 8000, 16000, 22050,
// 24000, 32000, 44100 or 48000 Hz. Anything else is refused before dialling,
// and Open also checks that sessionCreated echoes the requested rate, since
// audio at another rate would play at the wrong pitch with no error.
//
// # Utterances
//
// text.append maps to sendText and text.commit to a sendText flush. The adapter
// enables Kalpa's server-side segmentation (an empty generation_config), so
// audio for complete sentences starts while later text is still arriving; the
// utterance stays one response with one responseCreated and one responseDone.
// As on every sibling socket, one committed utterance is in flight at a time:
// text for the next one waits for audio.done. A commit with no text, or only
// whitespace, gets a completed response with no audio, so it yields audio.done
// without audio.started instead of failing the session. Kalpa caps one
// utterance at 8,000 characters; the adapter refuses a longer one locally as
// input_too_large.
//
// # Cancel
//
// Cancel sends a bare cancelResponse, which abandons the in-flight response and
// any buffered text, and returns at once: the session is usable for the next
// utterance immediately, and Cancel with nothing in flight is a no-op. Kalpa
// answers every cancelResponse with exactly one `cancelled`, after the last
// message of the work it stopped and before any later response starts, so the
// adapter drops every response message until that acknowledgement and then
// emits audio.done with cancelled=true for the stopped utterance.
//
// Close lets a committed utterance finish and cancels uncommitted text the same
// way, since segmentation may already have started speaking it.
//
// # Errors
//
// The gateway does not retry inside a session, so every error frame ends the
// session, including the response-scoped ones Kalpa's socket survives
// (overloaded, inference_error). Those are marked retryable, so the client can
// retry on a new session. context_length_exceeded maps to input_too_large.
// The one exception is a response-scoped error that arrives while a cancel is
// unacknowledged: it belongs to the work that cancel stopped and is dropped.
//
// # Idle sockets
//
// Kalpa closes a socket that receives no client message for several minutes
// (close 4408), and WebSocket pings do not count. Gateway sessions can wait
// longer between utterances, so the adapter sends an empty sendText, which
// changes nothing on the server, after each quiet minute.
//
// # Language
//
// kalpa-tts-1 speaks English and Hindi and reads the language from the text;
// the socket takes no language field. Other languages are refused at Open as
// unsupported_language.
//
// # Credentials
//
// Kalpa issues long-lived API keys only, with no short-lived token exchange, so
// the adapter accepts BYOK and Speko Relay plans and refuses managed
// provider-direct plans, where the key would reach a customer sidecar.
package kalpa
