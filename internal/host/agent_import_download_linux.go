package host

import (
	"context"
	"fmt"
	"net"
)

func downloadImportArchivePlatform(ctx context.Context, raw string, size uint64, send func([]byte) error) error {
	client, err := importHTTPClient(ctx, raw, net.DefaultResolver, (&net.Dialer{}).DialContext)
	if err != nil {
		return fmt.Errorf("resolve runtime OCI archive: %w", err)
	}
	return downloadImportArchiveWithClient(ctx, client, raw, size, send)
}
