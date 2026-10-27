//go:build darwin && e2e

package e2e

import (
	"context"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func testGoferFileCache(t *testing.T, client *productClient) {
	const namespace, image = "file-cache", "agent-vmm.local/file-cache:latest"
	ensureNamespace(t, client, namespace, 64<<20)
	archive, digest := buildExecutableArchive(t, "./test/e2e/testdata/filecache", "bin/file-cache", false)
	importTestImage(t, client, namespace, "file-cache-import", image, archive, digest)
	createVolume(t, client, namespace, "first")
	createVolume(t, client, namespace, "second")
	container := createContainerWithImageAndVolumes(t, client, namespace, "files", image,
		[]string{"/bin/file-cache"}, []*hostv1.VolumeMount{
			{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "first"}, Destination: "/first"},
			{Source: &hostv1.VolumeMount_OwnedVolume{OwnedVolume: "second"}, Destination: "/second"},
		})
	startContainer(t, client, namespace, container.GetId())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result, err := client.WaitContainer(ctx, namespace, &hostv1.WaitContainerRequest{ContainerId: container.GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if logs := readLogs(t, client, namespace, container.GetId()); result.GetExitCode() != 0 || string(logs) != "file cache workload completed\n" {
		t.Fatalf("file cache workload exit = %d, output = %q", result.GetExitCode(), logs)
	}
	deleteContainer(t, client, namespace, container.GetId())
	deleteVolume(t, client, namespace, "first")
	deleteVolume(t, client, namespace, "second")
	deleteNamespace(t, client, namespace)
}
