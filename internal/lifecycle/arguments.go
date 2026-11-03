//go:build darwin || linux

package lifecycle

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
)

type lifecycleUpdateArgs struct {
	sourceApp               string
	targetReleaseID         string
	requestID               string
	artifactSHA256          string
	artifactSize            uint64
	preparationFailureStdin bool
}

type updatePreparationFailureDescriptor struct {
	Version   int    `json:"version"`
	Stage     string `json:"stage"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type updatePreparationError struct {
	descriptor updatePreparationFailureDescriptor
}

func (failure updatePreparationError) Error() string { return failure.descriptor.Message }

func parseUpdateArgs(args []string) lifecycleUpdateArgs {
	var result lifecycleUpdateArgs
	for len(args) > 0 {
		switch args[0] {
		case "--preparation-failure-stdin":
			result.preparationFailureStdin = true
			args = args[1:]
			continue
		case "--source-app":
			if len(args) < 2 {
				fatal("--source-app requires a value")
			}
			result.sourceApp = args[1]
		case "--target-release-id":
			if len(args) < 2 {
				fatal("--target-release-id requires a value")
			}
			result.targetReleaseID = args[1]
		case "--request-id":
			if len(args) < 2 {
				fatal("--request-id requires a value")
			}
			result.requestID = args[1]
		case "--artifact-sha256":
			if len(args) < 2 {
				fatal("--artifact-sha256 requires a value")
			}
			result.artifactSHA256 = strings.ToLower(args[1])
		case "--artifact-size":
			if len(args) < 2 {
				fatal("--artifact-size requires a value")
			}
			parsed, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil || parsed == 0 {
				fatal("update artifact size is invalid")
			}
			result.artifactSize = parsed
		default:
			fatal("unknown update argument")
		}
		args = args[2:]
	}
	if result.targetReleaseID == "" || !operationIDPattern.MatchString(result.targetReleaseID) || result.requestID == "" || !operationIDPattern.MatchString(result.requestID) {
		fatal("update requires a valid --target-release-id and --request-id")
	}
	if result.preparationFailureStdin == (result.sourceApp != "") {
		fatal("update requires exactly one of --source-app or --preparation-failure-stdin")
	}
	if result.sourceApp != "" && !filepath.IsAbs(result.sourceApp) {
		fatal("update requires an absolute --source-app")
	}
	if (result.artifactSHA256 == "") != (result.artifactSize == 0) {
		fatal("update artifact SHA-256 and size must be supplied together")
	}
	if result.artifactSHA256 != "" {
		decoded, err := hex.DecodeString(result.artifactSHA256)
		if err != nil || len(decoded) != 32 {
			fatal("update artifact SHA-256 is invalid")
		}
	}
	if result.preparationFailureStdin && result.artifactSHA256 == "" {
		fatal("update preparation failure requires artifact SHA-256 and size")
	}
	return result
}

func decodeUpdatePreparationFailure(input io.Reader) (updatePreparationFailureDescriptor, error) {
	var result updatePreparationFailureDescriptor
	decoder := json.NewDecoder(io.LimitReader(input, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, errors.New("update preparation failure is invalid")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return result, errors.New("update preparation failure has trailing data")
	}
	if result.Version != 1 || result.Stage != "target_selection" && result.Stage != "download" && result.Stage != "preparation" ||
		!operationIDPattern.MatchString(result.Code) || strings.TrimSpace(result.Message) == "" || len(result.Message) > 512 || strings.ContainsAny(result.Message, "\r\n") {
		return result, errors.New("update preparation failure fields are invalid")
	}
	return result, nil
}

func parseInstallArgs(action string, args []string) (bool, string, bool, string, string) {
	disablePairing := false
	requestID := ""
	operationStdin := false
	resumeOperationID := ""
	registrationID := ""
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--disable-personal-mesh-pairing":
			disablePairing = true
		case "--operation-stdin", "--descriptor-stdin":
			if action != "install" && action != "registration-claim" {
				fatal("--operation-stdin is only valid for install")
			}
			operationStdin = true
		case "--resume-operation":
			if action != "install" || index+1 >= len(args) {
				fatal("--resume-operation requires an install operation id")
			}
			index++
			resumeOperationID = args[index]
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		case "--registration-id":
			if action != "repair" || index+1 >= len(args) {
				fatal("--registration-id is only valid for repair")
			}
			index++
			registrationID = args[index]
		default:
			fatal("install and repair accept --disable-personal-mesh-pairing, --request-id, repair --registration-id, and install --operation-stdin or --resume-operation")
		}
	}
	if registrationID != "" && !operationIDPattern.MatchString(registrationID) {
		fatal("--registration-id is invalid")
	}
	return disablePairing, requestID, operationStdin, resumeOperationID, registrationID
}

func requestIDArg(args []string) string {
	for index := 0; index < len(args); index++ {
		if args[index] == "--request-id" && index+1 < len(args) {
			return args[index+1]
		}
	}
	if len(args) != 0 {
		fatal("only --request-id is accepted")
	}
	return ""
}

func parseUninstallArgs(args []string) (bool, string) {
	purge := false
	requestID := ""
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--purge":
			purge = true
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		default:
			fatal("uninstall accepts --purge and --request-id")
		}
	}
	return purge, requestID
}
