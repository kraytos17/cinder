#include <cinder/store/lfu_store.hpp>
#include <cinder/store/lru_store.hpp>
#include <cstddef>
#include <cstdint>
#include <string_view>

using std::chrono::milliseconds;

// Helper: extract a TTL from trailing payload bytes. If >= 4 bytes remain
// after `offset`, interpret them as a uint32 and return a TTL (0 = no TTL,
// non-zero = that many ms). Otherwise returns 0 (no TTL).
static auto
extractTtl(std::string_view payload, size_t offset) -> std::optional<milliseconds> {
    if (payload.size() >= offset + 4) {
        uint32_t raw = 0;
        std::memcpy(&raw, payload.data() + offset, sizeof(raw));
        if (raw > 0) {
            return milliseconds(raw);
        }
    }
    return std::nullopt;
}

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 2) {
        return 0;
    }

    std::string_view input(reinterpret_cast<const char*>(data), size);
    unsigned sel = data[0];
    std::string_view payload(input.data() + 1, input.size() - 1);
    // Selector bits: 0-1 = store type (LRU/LFU), 2-4 = operation
    bool use_lfu = (sel & 0x01U) != 0;
    auto op = static_cast<uint8_t>((sel >> 1U) & 0x07U);
    if (use_lfu) {
        cinder::LfuStore store(4'096);
        switch (op) {
            case 0: {
                // put: split payload into key/value at first byte, optional TTL
                if (payload.size() < 1) {
                    break;
                }

                auto k = payload.substr(0, 1);
                auto v = payload.substr(1);
                auto ttl = extractTtl(payload, 1);
                // NOLINTNEXTLINE
                (void)store.put(std::string(k), std::string(v), ttl);
                break;
            }
            case 1: {
                // putVersioned: construct entry from payload bytes
                if (payload.size() < 17) {
                    break;
                }

                auto k = payload.substr(0, 1);
                auto v = payload.substr(1);
                cinder::VersionedEntry entry;
                entry.value = std::string(v);
                entry.version_and_ttl = 0;
                std::memcpy(&entry.version_and_ttl, data + 1, sizeof(uint64_t));
                entry.writer_node_hash = 0;
                std::memcpy(&entry.writer_node_hash, data + 9, sizeof(uint64_t));
                // NOLINTNEXTLINE
                (void)store.putVersioned(std::string(k), std::move(entry));
                break;
            }
            case 2: {
                // get
                if (payload.size() < 1) {
                    break;
                }
                (void)store.get(std::string(payload.substr(0, 1)));
                break;
            }
            case 3: {
                // getVersioned
                if (payload.size() < 1) {
                    break;
                }
                (void)store.getVersioned(std::string(payload.substr(0, 1)));
                break;
            }
            case 4: {
                // remove
                if (payload.size() < 1) {
                    break;
                }
                (void)store.remove(std::string(payload.substr(0, 1)));
                break;
            }
            case 5: {
                // put + get + remove cycle
                if (payload.size() < 1) {
                    break;
                }

                auto k = std::string(payload.substr(0, 1));
                // NOLINTNEXTLINE
                (void)store.put(k, std::string(payload.substr(1)));
                (void)store.get(k);
                (void)store.remove(k);
                break;
            }
            case 6: {
                // evictExpired
                (void)store.evictExpired();
                break;
            }
            case 7: {
                // size
                (void)store.size();
                break;
            }
            default:
                break;
        }
    } else {
        cinder::LruStore store(4'096);
        switch (op) {
            case 0: {
                if (payload.size() < 1) {
                    break;
                }

                auto k = payload.substr(0, 1);
                auto v = payload.substr(1);
                auto ttl = extractTtl(payload, 1);
                // NOLINTNEXTLINE
                (void)store.put(std::string(k), std::string(v), ttl);
                break;
            }
            case 1: {
                if (payload.size() < 17) {
                    break;
                }

                auto k = payload.substr(0, 1);
                auto v = payload.substr(1);
                cinder::VersionedEntry entry;
                entry.value = std::string(v);
                entry.version_and_ttl = 0;
                std::memcpy(&entry.version_and_ttl, data + 1, sizeof(uint64_t));
                entry.writer_node_hash = 0;
                std::memcpy(&entry.writer_node_hash, data + 9, sizeof(uint64_t));
                // NOLINTNEXTLINE
                (void)store.putVersioned(std::string(k), std::move(entry));
                break;
            }
            case 2: {
                if (payload.size() < 1) {
                    break;
                }
                (void)store.get(std::string(payload.substr(0, 1)));
                break;
            }
            case 3: {
                if (payload.size() < 1) {
                    break;
                }
                (void)store.getVersioned(std::string(payload.substr(0, 1)));
                break;
            }
            case 4: {
                if (payload.size() < 1) {
                    break;
                }
                (void)store.remove(std::string(payload.substr(0, 1)));
                break;
            }
            case 5: {
                if (payload.size() < 1) {
                    break;
                }

                auto k = std::string(payload.substr(0, 1));
                // NOLINTNEXTLINE
                (void)store.put(k, std::string(payload.substr(1)));
                (void)store.get(k);
                (void)store.remove(k);
                break;
            }
            case 6: {
                (void)store.evictExpired();
                break;
            }
            case 7: {
                (void)store.size();
                break;
            }
            default:
                break;
        }
    }
    return 0;
}
