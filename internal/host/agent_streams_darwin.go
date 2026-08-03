//go:build darwin

package host

import (
	"io"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
)

func (server *agentServer) Exec(stream hostv1.AgentRuntimeService_ExecServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.Exec(stream.Context())
	if err != nil {
		return err
	}
	receiveError := make(chan error, 1)
	go func() {
		for {
			request, err := stream.Recv()
			if err == io.EOF {
				receiveError <- guest.CloseSend()
				return
			}
			if err != nil {
				receiveError <- err
				return
			}
			if err := guest.Send(execRequestToGuest(request, identity.NamespaceID)); err != nil {
				receiveError <- err
				return
			}
		}
	}()
	for {
		response, err := guest.Recv()
		if err == io.EOF {
			return streamReceiveResult(receiveError)
		}
		if err != nil {
			return err
		}
		if err := stream.Send(execResponseFromGuest(response)); err != nil {
			return err
		}
	}
}

func execRequestToGuest(request *hostv1.ExecRequest, namespaceID string) *vmmdv1.ExecRequest {
	result := &vmmdv1.ExecRequest{}
	switch event := request.GetEvent().(type) {
	case *hostv1.ExecRequest_Start:
		start := event.Start
		result.Event = &vmmdv1.ExecRequest_Start{Start: &vmmdv1.ExecStart{
			RequestId: start.GetRequestId(), ActorNamespaceId: namespaceID, ContainerId: start.GetContainerId(), Argv: start.GetArgv(), Env: start.GetEnv(),
			Workdir: start.Workdir, User: start.User, Tty: start.GetTty(), Rows: start.GetRows(), Columns: start.GetColumns(),
		}}
	case *hostv1.ExecRequest_Stdin:
		result.Event = &vmmdv1.ExecRequest_Stdin{Stdin: event.Stdin}
	case *hostv1.ExecRequest_CloseStdin:
		result.Event = &vmmdv1.ExecRequest_CloseStdin{CloseStdin: event.CloseStdin}
	case *hostv1.ExecRequest_Resize:
		result.Event = &vmmdv1.ExecRequest_Resize{Resize: &vmmdv1.ExecResize{Rows: event.Resize.GetRows(), Columns: event.Resize.GetColumns()}}
	case *hostv1.ExecRequest_Signal:
		result.Event = &vmmdv1.ExecRequest_Signal{Signal: vmmdv1.ProcessSignal(event.Signal)}
	}
	return result
}

func execResponseFromGuest(response *vmmdv1.ExecResponse) *hostv1.ExecResponse {
	result := &hostv1.ExecResponse{}
	switch event := response.GetEvent().(type) {
	case *vmmdv1.ExecResponse_Stdout:
		result.Event = &hostv1.ExecResponse_Stdout{Stdout: event.Stdout}
	case *vmmdv1.ExecResponse_Stderr:
		result.Event = &hostv1.ExecResponse_Stderr{Stderr: event.Stderr}
	case *vmmdv1.ExecResponse_ExitCode:
		result.Event = &hostv1.ExecResponse_ExitCode{ExitCode: event.ExitCode}
	}
	return result
}

func (server *agentServer) ForwardTCP(stream hostv1.AgentRuntimeService_ForwardTCPServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.ForwardTCP(stream.Context())
	if err != nil {
		return err
	}
	receiveError := make(chan error, 1)
	go func() {
		for {
			request, err := stream.Recv()
			if err == io.EOF {
				receiveError <- guest.CloseSend()
				return
			}
			if err != nil {
				receiveError <- err
				return
			}
			converted := &vmmdv1.ForwardTCPRequest{}
			switch event := request.GetEvent().(type) {
			case *hostv1.ForwardTCPRequest_Header:
				converted.Event = &vmmdv1.ForwardTCPRequest_Header{Header: &vmmdv1.ForwardTCPHeader{ActorNamespaceId: identity.NamespaceID, ContainerId: event.Header.GetContainerId(), Port: event.Header.GetPort()}}
			case *hostv1.ForwardTCPRequest_Data:
				converted.Event = &vmmdv1.ForwardTCPRequest_Data{Data: event.Data}
			case *hostv1.ForwardTCPRequest_CloseWrite:
				converted.Event = &vmmdv1.ForwardTCPRequest_CloseWrite{CloseWrite: event.CloseWrite}
			}
			if err := guest.Send(converted); err != nil {
				receiveError <- err
				return
			}
		}
	}()
	for {
		response, err := guest.Recv()
		if err == io.EOF {
			return streamReceiveResult(receiveError)
		}
		if err != nil {
			return err
		}
		converted := &hostv1.ForwardTCPResponse{}
		switch event := response.GetEvent().(type) {
		case *vmmdv1.ForwardTCPResponse_Data:
			converted.Event = &hostv1.ForwardTCPResponse_Data{Data: event.Data}
		case *vmmdv1.ForwardTCPResponse_CloseRead:
			converted.Event = &hostv1.ForwardTCPResponse_CloseRead{CloseRead: event.CloseRead}
		}
		if err := stream.Send(converted); err != nil {
			return err
		}
	}
}

func (server *agentServer) BuildImage(stream hostv1.AgentRuntimeService_BuildImageServer) error {
	identity, err := auth.AgentIdentity(stream.Context())
	if err != nil {
		return err
	}
	client, connection, err := server.controller.guestClient(stream.Context())
	if err != nil {
		return err
	}
	defer connection.Close()
	guest, err := client.BuildImage(stream.Context())
	if err != nil {
		return err
	}
	receiveError := make(chan error, 1)
	go func() {
		for {
			request, err := stream.Recv()
			if err == io.EOF {
				receiveError <- guest.CloseSend()
				return
			}
			if err != nil {
				receiveError <- err
				return
			}
			if err := guest.Send(buildRequestToGuest(request, identity.NamespaceID)); err != nil {
				receiveError <- err
				return
			}
		}
	}()
	for {
		response, err := guest.Recv()
		if err == io.EOF {
			return streamReceiveResult(receiveError)
		}
		if err != nil {
			return err
		}
		converted := &hostv1.BuildImageResponse{}
		switch event := response.GetEvent().(type) {
		case *vmmdv1.BuildImageResponse_Progress:
			converted.Event = &hostv1.BuildImageResponse_Progress{Progress: event.Progress}
		case *vmmdv1.BuildImageResponse_Result:
			converted.Event = &hostv1.BuildImageResponse_Result{Result: &hostv1.BuildImageResult{Image: event.Result.GetImage(), Digest: event.Result.GetDigest()}}
		}
		if err := stream.Send(converted); err != nil {
			return err
		}
	}
}

func buildRequestToGuest(request *hostv1.BuildImageRequest, namespaceID string) *vmmdv1.BuildImageRequest {
	result := &vmmdv1.BuildImageRequest{}
	switch event := request.GetEvent().(type) {
	case *hostv1.BuildImageRequest_Header:
		header := event.Header
		converted := &vmmdv1.BuildImageHeader{RequestId: header.GetRequestId(), ActorNamespaceId: namespaceID, DockerfilePath: header.GetDockerfilePath(), Image: header.GetImage(), Target: header.Target, NetworkMode: vmmdv1.BuildNetworkMode(header.GetNetworkMode())}
		for _, argument := range header.GetBuildArgs() {
			converted.BuildArgs = append(converted.BuildArgs, &vmmdv1.BuildArgument{Name: argument.GetName(), Value: argument.GetValue()})
		}
		for _, secret := range header.GetSecrets() {
			converted.Secrets = append(converted.Secrets, &vmmdv1.BuildSecret{Id: secret.GetId()})
		}
		result.Event = &vmmdv1.BuildImageRequest_Header{Header: converted}
	case *hostv1.BuildImageRequest_Context:
		result.Event = &vmmdv1.BuildImageRequest_Context{Context: &vmmdv1.BuildContextChunk{Data: event.Context.GetData(), Final: event.Context.GetFinal()}}
	case *hostv1.BuildImageRequest_Secret:
		result.Event = &vmmdv1.BuildImageRequest_Secret{Secret: &vmmdv1.BuildSecretChunk{Id: event.Secret.GetId(), Data: event.Secret.GetData(), Final: event.Secret.GetFinal()}}
	}
	return result
}

func streamReceiveResult(channel <-chan error) error {
	select {
	case err := <-channel:
		return err
	default:
		return nil
	}
}
