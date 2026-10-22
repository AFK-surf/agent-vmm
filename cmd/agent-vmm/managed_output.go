//go:build darwin || linux

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

var managedSecretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(authorization\s*:\s*)[^\r\n]+`),
	regexp.MustCompile(`(?i)((?:set-)?cookie\s*:\s*)[^\r\n]+`),
	regexp.MustCompile(`(?i)("(?:token|secret|credential|password|api[_-]?key|access[_-]?key(?:_id)?|private[_-]?key|authorization|(?:set-)?cookie)"\s*:\s*)"[^"]*"`),
	regexp.MustCompile(`(?i)('(?:token|secret|credential|password|api[_-]?key|access[_-]?key(?:_id)?|private[_-]?key|authorization|(?:set-)?cookie)'\s*:\s*)'[^']*'`),
	regexp.MustCompile(`(?i)(["']?(?:token|secret|credential|password|api[_-]?key|access[_-]?key(?:_id)?|private[_-]?key|authorization|(?:set-)?cookie)["']?\s*[:=]\s*)[^\s,"']+`),
}

var managedIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
var managedSHA256Pattern = regexp.MustCompile(`^[A-Fa-f0-9]{64}$`)

func redactManagedText(value string) string {
	for _, pattern := range managedSecretPatterns {
		value = pattern.ReplaceAllString(value, `${1}[REDACTED]`)
	}
	return value
}

func redactManagedMap(value map[string]any) map[string]any {
	for key, item := range value {
		if managedSensitiveKey(key) {
			value[key] = "[REDACTED]"
		} else {
			value[key] = redactManagedValue(item)
		}
	}
	return value
}

func managedSensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	for _, part := range []string{"token", "secret", "credential", "password", "api_key", "access_key", "private_key", "authorization", "cookie", "task_body", "prompt", "private_content"} {
		if strings.Contains(normalized, part) {
			return true
		}
	}
	return false
}

func redactManagedValue(value any) any {
	switch typed := value.(type) {
	case string:
		return redactManagedText(typed)
	case map[string]any:
		return redactManagedMap(typed)
	case []any:
		for index, item := range typed {
			typed[index] = redactManagedValue(item)
		}
		return typed
	default:
		return value
	}
}

func managedBool(value bool) *bool {
	return &value
}

func optionalBoolFact(value map[string]any, key string) *bool {
	result, ok := value[key].(bool)
	if !ok {
		return nil
	}
	return managedBool(result)
}

func fitManagedLogEnvelope(envelope managedEnvelope) managedEnvelope {
	encoded, err := json.Marshal(envelope)
	if err != nil || len(encoded)+1 <= managedLogBytes {
		return envelope
	}
	envelope.Gap = true
	original := append([]managedLogRecord(nil), envelope.Logs...)
	for index := range envelope.Logs {
		envelope.Logs[index].Data = ""
		if original[index].Data != "" {
			envelope.Logs[index].Truncated = true
		}
	}
	local := len(envelope.Source) == 1 && (envelope.Source[0] == "guest_log" || envelope.Source[0] == "host_log")
	for step := range original {
		index := step
		if local {
			index = len(original) - 1 - step
		}
		value := []rune(strings.ToValidUTF8(original[index].Data, "�"))
		low, high := 0, len(value)
		for low < high {
			middle := low + (high-low+1)/2
			envelope.Logs[index].Data = string(value[:middle])
			if local {
				envelope.Logs[index].Data = string(value[len(value)-middle:])
			}
			candidate, marshalErr := json.Marshal(envelope)
			if marshalErr == nil && len(candidate)+1 <= managedLogBytes {
				low = middle
			} else {
				high = middle - 1
			}
		}
		envelope.Logs[index].Data = string(value[:low])
		if local {
			envelope.Logs[index].Data = string(value[len(value)-low:])
		}
		envelope.Logs[index].Truncated = original[index].Truncated || low < len(value)
		if low < len(value) {
			break
		}
	}
	return envelope
}

func boundedManagedText(value string, limit int) string {
	value = redactManagedText(strings.TrimSpace(value))
	if len(value) > limit {
		return value[:limit]
	}
	return value
}

func writeManagedError(stderr io.Writer, command, code, message string) {
	writeManagedErrorAtStage(stderr, command, code, message, "validation")
}

func writeManagedErrorAtStage(stderr io.Writer, command, code, message, stage string) {
	data, _ := json.Marshal(managedEnvelope{Version: managedEnvelopeVersion, Command: command, Target: map[string]any{}, Source: []string{}, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Freshness: "unknown", State: "failed", Stage: stage, Issue: managedIssue{Code: code, Message: boundedManagedText(message, 1024), Layer: "cli"}, OperationRef: nil, NextAction: []managedNextAction{}, Partial: true})
	fmt.Fprintln(stderr, string(data))
}

func boolFact(value map[string]any, key string) bool {
	result, _ := value[key].(bool)
	return result
}

func stringFact(value map[string]any, key string) string {
	result, _ := value[key].(string)
	return result
}

func numberFact(value map[string]any, key string) int64 {
	switch result := value[key].(type) {
	case float64:
		return int64(result)
	case int64:
		return result
	case int:
		return int64(result)
	default:
		return 0
	}
}

func firstNonEmptyManaged(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
