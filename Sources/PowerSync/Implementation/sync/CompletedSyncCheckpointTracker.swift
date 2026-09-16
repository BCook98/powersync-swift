import Foundation

enum CompletedSyncCheckpointTrackerError: Error, Equatable {
    case negativeLastOperationID
    case duplicateBucketName
    case contradictoryBucketChange
    case ambiguousCheckpointEnvelope
    case duplicateCheckpointEnvelopeKey
}

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
        try rejectRepeatedCheckpointEnvelopeKeys(in: line)

        let envelope = try StreamingSyncClient.jsonDecoder.decode(
            CheckpointProtocolEnvelope.self,
            from: Data(line.utf8)
        )

        guard envelope.checkpoint == nil || envelope.checkpointDiff == nil else {
            throw CompletedSyncCheckpointTrackerError.ambiguousCheckpointEnvelope
        }

        if let checkpoint = envelope.checkpoint {
            guard checkpoint.lastOpID >= 0 else {
                throw CompletedSyncCheckpointTrackerError.negativeLastOperationID
            }
            var nextBuckets: [[UInt8]: CompletedSyncCheckpoint.Bucket] = [:]
            for bucket in checkpoint.buckets {
                let key = Array(bucket.name.utf8)
                guard nextBuckets[key] == nil else {
                    throw CompletedSyncCheckpointTrackerError.duplicateBucketName
                }
                nextBuckets[key] = CompletedSyncCheckpoint.Bucket(
                    name: bucket.name,
                    checksum: bucket.checksum.value
                )
            }
            lastOpID = checkpoint.lastOpID
            buckets = nextBuckets
        } else if let diff = envelope.checkpointDiff {
            guard diff.lastOpID >= 0 else {
                throw CompletedSyncCheckpointTrackerError.negativeLastOperationID
            }

            var removedBucketKeys = Set<[UInt8]>()
            for name in diff.removedBuckets {
                guard removedBucketKeys.insert(Array(name.utf8)).inserted else {
                    throw CompletedSyncCheckpointTrackerError.duplicateBucketName
                }
            }

            var updatedBuckets: [[UInt8]: CompletedSyncCheckpoint.Bucket] = [:]
            for bucket in diff.updatedBuckets {
                let key = Array(bucket.name.utf8)
                guard updatedBuckets[key] == nil else {
                    throw CompletedSyncCheckpointTrackerError.duplicateBucketName
                }
                updatedBuckets[key] = CompletedSyncCheckpoint.Bucket(
                    name: bucket.name,
                    checksum: bucket.checksum.value
                )
            }
            guard removedBucketKeys.isDisjoint(with: updatedBuckets.keys) else {
                throw CompletedSyncCheckpointTrackerError.contradictoryBucketChange
            }
            guard lastOpID != nil else {
                return
            }

            var nextBuckets = buckets
            for key in removedBucketKeys {
                nextBuckets.removeValue(forKey: key)
            }
            for (key, bucket) in updatedBuckets {
                nextBuckets[key] = bucket
            }
            buckets = nextBuckets
            lastOpID = diff.lastOpID
        }
    }
}

private func rejectRepeatedCheckpointEnvelopeKeys(in line: String) throws {
    let scanner = TopLevelJSONObjectKeyScanner(bytes: Array(line.utf8))
    var seenCheckpointKeys = Set<String>()

    for key in try scanner.keys() where key == "checkpoint" || key == "checkpoint_diff" {
        guard seenCheckpointKeys.insert(key).inserted else {
            throw CompletedSyncCheckpointTrackerError.duplicateCheckpointEnvelopeKey
        }
    }
}

/// Preserves top-level JSON key occurrences before `Decodable` collapses repeated keys.
/// Malformed JSON is left for the typed decoder to reject.
private struct TopLevelJSONObjectKeyScanner {
    private let bytes: [UInt8]

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    func keys() throws -> [String] {
        var keys: [String] = []
        var closingBytes: [UInt8] = []
        var inString = false
        var isEscaped = false
        var keyStart: Int?
        var expectsRootKey = false

        for index in bytes.indices {
            let byte = bytes[index]
            if inString {
                if isEscaped {
                    isEscaped = false
                } else if byte == 0x5C { // \
                    isEscaped = true
                } else if byte == 0x22 { // "
                    inString = false
                    if let keyStart {
                        let key = try StreamingSyncClient.jsonDecoder.decode(
                            String.self,
                            from: Data(bytes[keyStart...index])
                        )
                        keys.append(key)
                        expectsRootKey = false
                    }
                    keyStart = nil
                }
                continue
            }

            switch byte {
            case 0x22: // "
                inString = true
                if closingBytes.count == 1, closingBytes[0] == 0x7D, expectsRootKey {
                    keyStart = index
                }
            case 0x7B: // {
                closingBytes.append(0x7D)
                if closingBytes.count == 1 {
                    expectsRootKey = true
                }
            case 0x5B: // [
                closingBytes.append(0x5D)
            case 0x2C where closingBytes.count == 1 && closingBytes[0] == 0x7D:
                expectsRootKey = true
            case 0x7D, 0x5D:
                guard closingBytes.last == byte else {
                    break
                }
                closingBytes.removeLast()
                if closingBytes.isEmpty {
                    return keys
                }
            default:
                break
            }
        }
        return keys
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
