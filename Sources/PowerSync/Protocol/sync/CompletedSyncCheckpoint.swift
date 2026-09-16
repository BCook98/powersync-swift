/// An immutable public snapshot of a fully applied PowerSync checkpoint.
///
/// This value is derived from the public sync protocol. It is published only after the
/// corresponding checkpoint has been successfully applied to the local database.
public struct CompletedSyncCheckpoint: Sendable, Equatable {
    /// One bucket and its checksum at the completed checkpoint.
    public struct Bucket: Sendable, Equatable {
        public let name: String
        public let checksum: UInt32

        public init(name: String, checksum: UInt32) {
            self.name = name
            self.checksum = checksum
        }
    }

    /// The checkpoint's last operation identifier.
    public let lastOpID: Int64

    /// Bucket checksums ordered lexicographically by the raw UTF-8 bytes of each name.
    public let buckets: [Bucket]

    public init(lastOpID: Int64, buckets: [Bucket]) {
        self.lastOpID = lastOpID
        self.buckets = buckets.sorted { left, right in
            left.name.utf8.lexicographicallyPrecedes(right.name.utf8)
        }
    }
}
