package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const model = "gpt-5.6"

type config struct {
	Providers []provider `json:"providers"`
}
type provider struct {
	ID     string `json:"id"`
	Config struct {
		APIKey string `json:"apiKey"`
	} `json:"config"`
}
type metrics struct {
	Mode                   string `json:"mode"`
	Success                bool   `json:"success"`
	Calls                  int    `json:"calls"`
	Turns                  int    `json:"turns"`
	Programs               int    `json:"programs"`
	InputTokens            int    `json:"input_tokens"`
	OutputTokens           int    `json:"output_tokens"`
	LatencyMS              int64  `json:"latency_ms"`
	ProgramOutputValidated bool   `json:"program_output_validated"`
	MessageValidated       bool   `json:"message_validated"`
}

var implementations = map[string]any{
	"environment_get": map[string]any{"ok": true, "data": map[string]any{"origin": "local", "health": "healthy", "containers": 3, "volumes": 2}},
	"container_list":  map[string]any{"ok": true, "data": map[string]any{"items": []map[string]any{{"id": "api", "state": "running"}, {"id": "job", "state": "stopped"}, {"id": "worker", "state": "running"}}}},
	"volume_list":     map[string]any{"ok": true, "data": map[string]any{"items": []map[string]any{{"id": "cache"}, {"id": "state"}}}},
}

func main() {
	path := ".context/token.json"
	if len(os.Args) == 2 {
		path = os.Args[1]
	}
	key, err := readKey(path)
	if err != nil {
		fail(err)
	}
	results := make([]metrics, 0, 2)
	for _, programmatic := range []bool{false, true} {
		result, err := run(key, programmatic)
		if err != nil {
			fail(err)
		}
		results = append(results, result)
	}
	payload, _ := json.MarshalIndent(map[string]any{"model": model, "results": results}, "", "  ")
	fmt.Println(string(payload))
}

func readKey(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New("token config unavailable")
	}
	var value config
	if err := json.Unmarshal(data, &value); err != nil {
		return "", errors.New("token config is invalid")
	}
	for _, provider := range value.Providers {
		if provider.ID == "openai" && provider.Config.APIKey != "" {
			return provider.Config.APIKey, nil
		}
	}
	return "", errors.New("OpenAI token is unavailable")
}

func run(key string, programmatic bool) (metrics, error) {
	mode := "direct"
	if programmatic {
		mode = "programmatic"
	}
	result := metrics{Mode: mode}
	tools := toolDefinitions(programmatic)
	input := []any{map[string]any{"role": "user", "content": `Inspect the environment, containers, and volumes. Return exactly one JSON object with health, running_ids sorted lexicographically, running_count, and volume_count. Use every available read tool once. Do not call mutations.`}}
	started := time.Now()
	for result.Turns < 8 {
		result.Turns++
		body := map[string]any{"model": model, "store": false, "input": input, "tools": tools, "reasoning": map[string]any{"effort": "low"}}
		response, err := request(key, body)
		if err != nil {
			return result, err
		}
		if usage, ok := response["usage"].(map[string]any); ok {
			result.InputTokens += number(usage["input_tokens"])
			result.OutputTokens += number(usage["output_tokens"])
		}
		output, _ := response["output"].([]any)
		input = append(input, output...)
		var calls []map[string]any
		message := ""
		for _, raw := range output {
			item, _ := raw.(map[string]any)
			switch item["type"] {
			case "function_call":
				calls = append(calls, item)
			case "program":
				result.Programs++
			case "program_output":
				if validAggregate(fmt.Sprint(item["result"])) {
					result.ProgramOutputValidated = true
				}
			case "message":
				message = messageText(item)
			}
		}
		if len(calls) != 0 {
			for _, call := range calls {
				name := fmt.Sprint(call["name"])
				value, ok := implementations[name]
				if !ok {
					return result, errors.New("model called a non-read tool")
				}
				encoded, _ := json.Marshal(value)
				item := map[string]any{"type": "function_call_output", "call_id": call["call_id"], "output": string(encoded)}
				if caller := call["caller"]; caller != nil {
					item["caller"] = caller
				}
				input = append(input, item)
				result.Calls++
			}
			continue
		}
		if message != "" {
			result.MessageValidated = validAggregate(message)
			result.Success = result.MessageValidated && (!programmatic || result.ProgramOutputValidated) && result.Calls == 3
			result.LatencyMS = time.Since(started).Milliseconds()
			if !result.Success {
				return result, errors.New(mode + " response failed validation")
			}
			return result, nil
		}
	}
	return result, errors.New(mode + " response exceeded turn limit")
}

func toolDefinitions(programmatic bool) []any {
	caller := []string{"direct"}
	if programmatic {
		caller = []string{"programmatic"}
	}
	empty := map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}
	environment := map[string]any{"type": "object", "required": []string{"ok", "data"}, "properties": map[string]any{"ok": map[string]any{"type": "boolean"}, "data": map[string]any{"type": "object", "required": []string{"origin", "health", "containers", "volumes"}, "properties": map[string]any{"origin": map[string]any{"type": "string"}, "health": map[string]any{"type": "string"}, "containers": map[string]any{"type": "integer"}, "volumes": map[string]any{"type": "integer"}}, "additionalProperties": false}}, "additionalProperties": false}
	container := map[string]any{"type": "object", "required": []string{"ok", "data"}, "properties": map[string]any{"ok": map[string]any{"type": "boolean"}, "data": map[string]any{"type": "object", "required": []string{"items"}, "properties": map[string]any{"items": map[string]any{"type": "array", "items": map[string]any{"type": "object", "required": []string{"id", "state"}, "properties": map[string]any{"id": map[string]any{"type": "string"}, "state": map[string]any{"type": "string"}}, "additionalProperties": false}}}, "additionalProperties": false}}, "additionalProperties": false}
	volume := map[string]any{"type": "object", "required": []string{"ok", "data"}, "properties": map[string]any{"ok": map[string]any{"type": "boolean"}, "data": map[string]any{"type": "object", "required": []string{"items"}, "properties": map[string]any{"items": map[string]any{"type": "array", "items": map[string]any{"type": "object", "required": []string{"id"}, "properties": map[string]any{"id": map[string]any{"type": "string"}}, "additionalProperties": false}}}, "additionalProperties": false}}, "additionalProperties": false}
	result := []any{
		map[string]any{"type": "function", "name": "environment_get", "description": "Return environment health and resource counts.", "parameters": empty, "output_schema": environment, "strict": true, "allowed_callers": caller},
		map[string]any{"type": "function", "name": "container_list", "description": "Return all containers and states.", "parameters": empty, "output_schema": container, "strict": true, "allowed_callers": caller},
		map[string]any{"type": "function", "name": "volume_list", "description": "Return all named volumes.", "parameters": empty, "output_schema": volume, "strict": true, "allowed_callers": caller},
	}
	if programmatic {
		result = append(result, map[string]any{"type": "programmatic_tool_calling"})
	}
	return result
}

func request(key string, value any) (map[string]any, error) {
	payload, _ := json.Marshal(value)
	request, _ := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/responses", bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 2 * time.Minute}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("OpenAI request failed")
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if response.StatusCode != http.StatusOK {
		var failure struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &failure)
		return nil, fmt.Errorf("OpenAI request failed: status %d code %s", response.StatusCode, failure.Error.Code)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, errors.New("OpenAI response was invalid")
	}
	return result, nil
}

func messageText(item map[string]any) string {
	content, _ := item["content"].([]any)
	result := ""
	for _, raw := range content {
		part, _ := raw.(map[string]any)
		if part["type"] == "output_text" {
			result += fmt.Sprint(part["text"])
		}
	}
	return result
}
func validAggregate(text string) bool {
	start, end := bytes.IndexByte([]byte(text), '{'), bytes.LastIndexByte([]byte(text), '}')
	if start < 0 || end < start {
		return false
	}
	var value struct {
		Health       string   `json:"health"`
		RunningIDs   []string `json:"running_ids"`
		RunningCount int      `json:"running_count"`
		VolumeCount  int      `json:"volume_count"`
	}
	if json.Unmarshal([]byte(text[start:end+1]), &value) != nil {
		return false
	}
	return value.Health == "healthy" && len(value.RunningIDs) == 2 && value.RunningIDs[0] == "api" && value.RunningIDs[1] == "worker" && value.RunningCount == 2 && value.VolumeCount == 2
}
func number(value any) int { result, _ := value.(float64); return int(result) }
func fail(err error)       { fmt.Fprintln(os.Stderr, "mcp eval:", err); os.Exit(1) }
