import Foundation

/// Tracks checkpoint state from accepted public sync-protocol lines.
///
/// The tracker deliberately has no database or logging dependency. Its current value is
/// only a candidate until the core reports that the checkpoint was successfully applied.
struct CompletedSyncCheckpointTracker {
    private var lastOpID: Int64?
    private var buckets: [[UInt8]: CompletedSyncCheckpoint.Bucket] = [:]

    var current: CompletedSyncCheckpoint? {
        guard let lastOpID else {
            return nil
        }

        return CompletedSyncCheckpoint(
            lastOpID: lastOpID,
            buckets: Array(buckets.values)
        )
    }

    mutating func receiveAcceptedProtocolLine(_ line: String) throws {
        let envelope = try StreamingSyncClient.jsonDecoder.decode(
            CheckpointProtocolEnvelope.self,
            from: Data(line.utf8)
        )

        if let checkpoint = envelope.checkpoint {
            lastOpID = checkpoint.lastOpID
            buckets = Dictionary(
                checkpoint.buckets.map {
                    (
                        Array($0.name.utf8),
                        CompletedSyncCheckpoint.Bucket(name: $0.name, checksum: $0.checksum.value)
                    )
                },
                uniquingKeysWith: { _, replacement in replacement }
            )
        } else if let diff = envelope.checkpointDiff {
            guard lastOpID != nil else {
                return
            }

            for name in diff.removedBuckets {
                buckets.removeValue(forKey: Array(name.utf8))
            }
            for bucket in diff.updatedBuckets {
                buckets[Array(bucket.name.utf8)] = CompletedSyncCheckpoint.Bucket(
                    name: bucket.name,
                    checksum: bucket.checksum.value
                )
            }
            lastOpID = diff.lastOpID
        }
    }
}

private struct CheckpointProtocolEnvelope: Decodable {
    let checkpoint: ProtocolCheckpoint?
    let checkpointDiff: ProtocolCheckpointDiff?

    enum CodingKeys: String, CodingKey {
        case checkpoint
        case checkpointDiff = "checkpoint_diff"
    }
}

private struct ProtocolCheckpoint: Decodable {
    @StringEncodedInt64 var lastOpID: Int64
    let buckets: [ProtocolBucketChecksum]

    enum CodingKeys: String, CodingKey {
        case lastOpID = "last_op_id"
        case buckets
    }
}

private struct ProtocolCheckpointDiff: Decodable {
    @StringEncodedInt64 var lastOpID: Int64
    let updatedBuckets: [ProtocolBucketChecksum]
    let removedBuckets: [String]

    enum CodingKeys: String, CodingKey {
        case lastOpID = "last_op_id"
        case updatedBuckets = "updated_buckets"
        case removedBuckets = "removed_buckets"
    }
}

private struct ProtocolBucketChecksum: Decodable {
    let name: String
    let checksum: ProtocolChecksum

    enum CodingKeys: String, CodingKey {
        case name = "bucket"
        case checksum
    }
}

/// Mirrors the service's checksum representation: either an unsigned 32-bit value or
/// the signed 32-bit integer with the same bits.
private struct ProtocolChecksum: Decodable {
    let value: UInt32

    init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()

        if let unsigned = try? container.decode(UInt32.self) {
            value = unsigned
            return
        }

        if let signed = try? container.decode(Int32.self) {
            value = UInt32(bitPattern: signed)
            return
        }

        let floatingPoint = try container.decode(Double.self)
        guard floatingPoint.isFinite,
              floatingPoint.rounded(.towardZero) == floatingPoint,
              floatingPoint >= Double(Int32.min),
              floatingPoint <= Double(UInt32.max)
        else {
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "Expected a 32-bit integer checksum"
            )
        }

        if floatingPoint < 0 {
            value = UInt32(bitPattern: Int32(floatingPoint))
        } else {
            value = UInt32(floatingPoint)
        }
    }
}
