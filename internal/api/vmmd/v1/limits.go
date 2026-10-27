package vmmdv1

// MaxImportArchiveChunkBytes stays below the Guest gRPC receive limit and is
// shared by both ends of the Host-to-Guest import stream.
const MaxImportArchiveChunkBytes = 3 << 20

// MaxContainerPageSize bounds one indexed container inspection response, not the
// number of containers that an environment can hold.
const MaxContainerPageSize = 32
