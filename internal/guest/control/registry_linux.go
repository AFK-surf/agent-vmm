//go:build linux

package control

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	guestimage "github.com/AFK-surf/agent-vmm/internal/guest/image"
	containerd "github.com/containerd/containerd/v2/client"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (r *Runtime) PullImage(ctx context.Context, namespaceID, imageReference string) (string, string, uint64, error) {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return "", "", 0, err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return "", "", 0, err
	}
	canonical, err := normalizeImageReference(imageReference)
	if err != nil {
		return "", "", 0, imageError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image reference is invalid", imageReference)
	}
	nsctx := namespaceContext(ctx, namespaceID)
	if existing, err := r.client.GetImage(nsctx, canonical); err == nil {
		unpacked, unpackErr := existing.IsUnpacked(nsctx, containerSnapshotter)
		if unpackErr != nil {
			return "", "", 0, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image unpack state could not be read", canonical)
		}
		if !unpacked {
			if unpackErr := existing.Unpack(nsctx, containerSnapshotter); unpackErr != nil {
				return "", "", 0, imageError(codes.FailedPrecondition, "IMAGE_UNPACK_FAILED", "image content could not be unpacked", canonical)
			}
		}
		size, err := existing.Size(nsctx)
		if err != nil {
			return "", "", 0, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image metadata could not be read", canonical)
		}
		return canonical, existing.Target().Digest.String(), uint64(size), nil
	} else if !errdefs.IsNotFound(err) {
		return "", "", 0, err
	}
	currentBytes, err := r.namespaceImageBytes(nsctx)
	if err != nil {
		return "", "", 0, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return "", "", 0, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return "", "", 0, err
	}
	remoteBytes, err := remoteImageSize(ctx, canonical, publicRegistryResolver)
	if err != nil {
		return "", "", 0, err
	}
	if currentBytes+remoteBytes > namespace.GetLimits().GetDiskBytes() {
		return "", "", 0, resourceExhausted("image logical size exceeds namespace disk quota")
	}
	var image containerd.Image
	err = retryRegistry(ctx, retryableRegistryError, func(attempt int) error {
		registryClient, resolver := publicRegistryResolver()
		image, err = r.client.Pull(nsctx, canonical, containerd.WithResolver(resolver), containerd.WithPlatform("linux/arm64"), containerd.WithMaxConcurrentDownloads(1))
		registryClient.CloseIdleConnections()
		if err != nil {
			log.Printf("registry pull failed for %q at attempt %d: %v", canonical, attempt, err)
		}
		return err
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", "", 0, ctxErr
		}
		if !retryableRegistryError(err) {
			return "", "", 0, registryPullError(err, canonical)
		}
		return "", "", 0, registryDownloadError(err, canonical)
	}
	if err := image.Unpack(nsctx, containerSnapshotter); err != nil {
		_ = r.client.ImageService().Delete(nsctx, image.Name(), coreimages.SynchronousDelete())
		return "", "", 0, imageError(codes.FailedPrecondition, "IMAGE_UNPACK_FAILED", "image content could not be unpacked", canonical)
	}
	size, err := image.Size(nsctx)
	if err != nil {
		return "", "", 0, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image metadata could not be read", canonical)
	}
	logicalBytes, err := r.namespaceImageBytes(nsctx)
	if err != nil {
		return "", "", 0, err
	}
	if logicalBytes > namespace.GetLimits().GetDiskBytes() {
		_ = r.client.ImageService().Delete(nsctx, image.Name(), coreimages.SynchronousDelete())
		return "", "", 0, resourceExhausted("image logical size exceeds namespace disk quota")
	}
	return canonical, image.Target().Digest.String(), uint64(size), nil
}

func normalizeImageReference(value string) (string, error) {
	return guestimage.NormalizeReference(value)
}

type registryResolverFactory func() (*http.Client, remotes.Resolver)

func remoteImageSize(ctx context.Context, reference string, resolverFactory registryResolverFactory) (uint64, error) {
	var size uint64
	err := retryRegistry(ctx, func(err error) bool { return status.Code(err) == codes.Unavailable }, func(attempt int) error {
		client, resolver := resolverFactory()
		var err error
		size, err = remoteImageSizeOnce(ctx, resolver, reference, attempt)
		client.CloseIdleConnections()
		return err
	})
	if err != nil {
		return 0, err
	}
	return size, nil
}

func remoteImageSizeOnce(ctx context.Context, resolver remotes.Resolver, reference string, attempt int) (uint64, error) {
	_, descriptor, err := resolver.Resolve(ctx, reference)
	if err != nil {
		log.Printf("registry resolve failed for %q at attempt %d: %v", reference, attempt, err)
		if errdefs.IsNotFound(err) {
			return 0, imageError(codes.NotFound, "IMAGE_NOT_FOUND", "image reference does not exist", reference)
		}
		if errdefs.IsPermissionDenied(err) {
			return 0, imageError(codes.PermissionDenied, "IMAGE_REGISTRY_DENIED", "image registry denied access", reference)
		}
		if errdefs.IsInvalidArgument(err) {
			return 0, imageError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image reference is invalid", reference)
		}
		return 0, imageError(codes.Unavailable, "IMAGE_REGISTRY_UNAVAILABLE", "image registry could not be reached", reference)
	}
	fetcher, err := resolver.Fetcher(ctx, reference)
	if err != nil {
		log.Printf("registry fetcher failed for %q at attempt %d: %v", reference, attempt, err)
		return 0, imageError(codes.Unavailable, "IMAGE_REGISTRY_UNAVAILABLE", "image registry is unavailable", reference)
	}
	total := uint64(descriptor.Size)
	if descriptor.MediaType == ocispec.MediaTypeImageIndex || descriptor.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json" {
		var index ocispec.Index
		if err := fetchJSON(ctx, fetcher, descriptor, &index); err != nil {
			log.Printf("registry index fetch failed for %q at attempt %d: %v", reference, attempt, err)
			return 0, imageError(codes.Unavailable, "IMAGE_MANIFEST_FETCH_FAILED", "image index could not be downloaded", reference)
		}
		var selected *ocispec.Descriptor
		for manifestIndex := range index.Manifests {
			candidate := &index.Manifests[manifestIndex]
			if candidate.Platform != nil && candidate.Platform.OS == "linux" && candidate.Platform.Architecture == "arm64" {
				selected = candidate
				break
			}
		}
		if selected == nil {
			return 0, imageError(codes.FailedPrecondition, "IMAGE_PLATFORM_UNAVAILABLE", "image has no linux/arm64 variant", reference)
		}
		descriptor = *selected
		total += uint64(descriptor.Size)
	}
	var manifest ocispec.Manifest
	if err := fetchJSON(ctx, fetcher, descriptor, &manifest); err != nil {
		log.Printf("registry manifest fetch failed for %q at attempt %d: %v", reference, attempt, err)
		return 0, imageError(codes.Unavailable, "IMAGE_MANIFEST_FETCH_FAILED", "image manifest could not be downloaded", reference)
	}
	total += uint64(manifest.Config.Size)
	for _, layer := range manifest.Layers {
		total += uint64(layer.Size)
	}
	return total, nil
}

func publicRegistryResolver() (*http.Client, remotes.Resolver) {
	client := publicRegistryClient()
	authorizer := docker.NewDockerAuthorizer(docker.WithAuthClient(client))
	hosts := docker.ConfigureDefaultRegistries(docker.WithClient(client), docker.WithAuthorizer(authorizer))
	return client, docker.NewResolver(docker.ResolverOptions{Hosts: hosts})
}

func retryableRegistryError(err error) bool {
	return !errdefs.IsNotFound(err) && !errdefs.IsPermissionDenied(err) && !errdefs.IsInvalidArgument(err)
}

func registryPullError(err error, reference string) error {
	switch {
	case errdefs.IsNotFound(err):
		return imageError(codes.NotFound, "IMAGE_NOT_FOUND", "image reference does not exist", reference)
	case errdefs.IsPermissionDenied(err):
		return imageError(codes.PermissionDenied, "IMAGE_REGISTRY_DENIED", "image registry denied access", reference)
	default:
		return imageError(codes.InvalidArgument, "IMAGE_REFERENCE_INVALID", "image reference is invalid", reference)
	}
}

func registryDownloadError(err error, reference string) error {
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "429") || strings.Contains(message, "too many requests"):
		return imageError(codes.ResourceExhausted, "IMAGE_REGISTRY_RATE_LIMITED", "image registry rate limit was reached", reference)
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(message, "timeout") || strings.Contains(message, "deadline exceeded"):
		return imageError(codes.Unavailable, "IMAGE_DOWNLOAD_TIMEOUT", "image download timed out", reference)
	case strings.Contains(message, "failed to authorize") || strings.Contains(message, "authorization") || strings.Contains(message, "token"):
		return imageError(codes.Unavailable, "IMAGE_AUTH_UNAVAILABLE", "image registry authentication was interrupted", reference)
	case errors.Is(err, io.ErrUnexpectedEOF), strings.Contains(message, "unexpected eof"), strings.Contains(message, "connection reset"), strings.Contains(message, "broken pipe"), strings.Contains(message, "tls handshake"):
		return imageError(codes.Unavailable, "IMAGE_DOWNLOAD_INTERRUPTED", "image download was interrupted", reference)
	default:
		return imageError(codes.Unavailable, "IMAGE_BLOB_FETCH_FAILED", "image content could not be downloaded", reference)
	}
}

func waitRegistryRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt+1) * 500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryRegistry(ctx context.Context, shouldRetry func(error) bool, operation func(attempt int) error) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		err = operation(attempt)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !shouldRetry(err) {
			return err
		}
		if attempt < 3 {
			if waitErr := waitRegistryRetry(ctx, attempt-1); waitErr != nil {
				return waitErr
			}
		}
	}
	return err
}

func fetchJSON(ctx context.Context, fetcher remotes.Fetcher, descriptor ocispec.Descriptor, target any) error {
	reader, err := fetcher.Fetch(ctx, descriptor)
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(io.LimitReader(reader, 16<<20)).Decode(target)
}

func (r *Runtime) namespaceImageBytes(ctx context.Context) (uint64, error) {
	images, err := r.client.ListImages(ctx)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]bool)
	var total uint64
	for _, image := range images {
		digest := image.Target().Digest.String()
		if seen[digest] {
			continue
		}
		size, err := image.Size(ctx)
		if err != nil {
			return 0, err
		}
		seen[digest] = true
		total += uint64(size)
	}
	return total, nil
}

func publicRegistryClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := docker.DefaultHTTPTransport(nil).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		addresses, err := resolvePublic(ctx, host)
		if err != nil {
			return nil, err
		}
		var dialError error
		for _, resolved := range addresses {
			connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(resolved.String(), port))
			if err == nil {
				return connection, nil
			}
			dialError = errors.Join(dialError, err)
		}
		return nil, dialError
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Minute}
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 || request.URL.Scheme != "https" {
			return errors.New("registry redirect is not an allowed HTTPS target")
		}
		_, err := resolvePublic(request.Context(), request.URL.Hostname())
		return err
	}
	return client
}

func resolvePublic(ctx context.Context, host string) ([]netip.Addr, error) {
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("registry host %q has no address", host)
	}
	for _, address := range addresses {
		if !publicAddress(address) {
			return nil, fmt.Errorf("registry host %q resolves to non-public address %s", host, address)
		}
	}
	return addresses, nil
}
