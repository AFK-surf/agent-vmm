//go:build linux

package control

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/errdefs"
	"github.com/distribution/reference"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func (r *Runtime) PullImage(ctx context.Context, namespaceID, imageReference string) (string, uint64, error) {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return "", 0, err
	}
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExists(ctx, namespaceID); err != nil {
		return "", 0, err
	}
	named, err := reference.ParseNormalizedNamed(imageReference)
	if err != nil || strings.Contains(imageReference, "://") || strings.Contains(imageReference, "@http") {
		return "", 0, errors.New("image reference must name a public HTTPS registry")
	}
	canonical := reference.TagNameOnly(named).String()
	client := publicRegistryClient()
	authorizer := docker.NewDockerAuthorizer(docker.WithAuthClient(client))
	hosts := docker.ConfigureDefaultRegistries(docker.WithClient(client), docker.WithAuthorizer(authorizer))
	resolver := docker.NewResolver(docker.ResolverOptions{Hosts: hosts})
	nsctx := namespaceContext(ctx, namespaceID)
	if existing, err := r.client.GetImage(nsctx, canonical); err == nil {
		size, err := existing.Size(nsctx)
		if err != nil {
			return "", 0, err
		}
		return existing.Target().Digest.String(), uint64(size), nil
	} else if !errdefs.IsNotFound(err) {
		return "", 0, err
	}
	currentBytes, err := r.namespaceImageBytes(nsctx)
	if err != nil {
		return "", 0, err
	}
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return "", 0, err
	}
	_, namespace, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return "", 0, err
	}
	remoteBytes, err := remoteImageSize(ctx, resolver, canonical)
	if err != nil {
		return "", 0, err
	}
	if currentBytes+remoteBytes > namespace.GetLimits().GetDiskBytes() {
		return "", 0, errors.New("image logical size exceeds namespace disk quota")
	}
	image, err := r.client.Pull(nsctx, canonical, containerd.WithResolver(resolver), containerd.WithPlatform("linux/arm64"), containerd.WithPullSnapshotter(containerSnapshotter), containerd.WithPullUnpack)
	if err != nil {
		return "", 0, err
	}
	size, err := image.Size(nsctx)
	if err != nil {
		return "", 0, err
	}
	logicalBytes, err := r.namespaceImageBytes(nsctx)
	if err != nil {
		return "", 0, err
	}
	if logicalBytes > namespace.GetLimits().GetDiskBytes() {
		_ = r.client.ImageService().Delete(nsctx, image.Name(), coreimages.SynchronousDelete())
		return "", 0, errors.New("image logical size exceeds namespace disk quota")
	}
	return image.Target().Digest.String(), uint64(size), nil
}

func remoteImageSize(ctx context.Context, resolver remotes.Resolver, reference string) (uint64, error) {
	_, descriptor, err := resolver.Resolve(ctx, reference)
	if err != nil {
		return 0, err
	}
	fetcher, err := resolver.Fetcher(ctx, reference)
	if err != nil {
		return 0, err
	}
	total := uint64(descriptor.Size)
	if descriptor.MediaType == ocispec.MediaTypeImageIndex || descriptor.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json" {
		var index ocispec.Index
		if err := fetchJSON(ctx, fetcher, descriptor, &index); err != nil {
			return 0, err
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
			return 0, errors.New("image index has no linux/arm64 manifest")
		}
		descriptor = *selected
		total += uint64(descriptor.Size)
	}
	var manifest ocispec.Manifest
	if err := fetchJSON(ctx, fetcher, descriptor, &manifest); err != nil {
		return 0, err
	}
	total += uint64(manifest.Config.Size)
	for _, layer := range manifest.Layers {
		total += uint64(layer.Size)
	}
	return total, nil
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
