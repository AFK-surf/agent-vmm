package vmmdv1

// MaxImportArchiveChunkBytes stays below the Guest gRPC receive limit and is
// shared by both ends of the Host-to-Guest import stream.
const MaxImportArchiveChunkBytes = 3 << 20
