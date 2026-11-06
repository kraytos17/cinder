#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "cinder/store/wal.hpp"
#include "fuzz_file_io.hpp"

namespace {

    // File-backed harness shared with snapshot_fuzz (see fuzz_file_io.hpp).
    cinder_fuzz::TempGarden garden("wal", "wal_", ".wal");

    auto operator==(const cinder::WalEntry& a, const cinder::WalEntry& b) -> bool {
        return a.op
            == b.op
            && a.key
            == b.key
            && a.value
            == b.value
            && a.version
            == b.version
            && a.writer_node_hash
            == b.writer_node_hash
            && a.expires_at_ms
            == b.expires_at_ms
            && a.has_ttl
            == b.has_ttl;
    }

    // Header is magic(4) + version(4): mutations below this may flip the legacy
    // fallback (no checksums, shifted framing), so only crash-freedom applies.
    constexpr size_t K_HEADER_SIZE = 8;

    auto makeEntry(FuzzedDataProvider& fdp) -> cinder::WalEntry {
        cinder::WalEntry e;
        e.op = fdp.PickValueInArray({cinder::WalEntry::Op::Set, cinder::WalEntry::Op::Del});
        e.key = fdp.ConsumeRandomLengthString(256);
        // Occasional large value exercises the capacity/size paths.
        e.value = fdp.ConsumeBool() ? fdp.ConsumeRandomLengthString(256)
                                    : fdp.ConsumeRandomLengthString(16'384);
        e.version = fdp.ConsumeIntegral<cinder::Version>();
        e.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
        e.expires_at_ms = fdp.ConsumeIntegral<uint64_t>();
        e.has_ttl = fdp.ConsumeBool();
        return e;
    }

    void roundtrip(FuzzedDataProvider& fdp) {
        const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 16);
        std::vector<cinder::WalEntry> entries;
        entries.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            entries.push_back(makeEntry(fdp));
        }

        auto guard = garden.next();
        {
            cinder::WalWriter writer(guard.path());
            for (const auto& e : entries) {
                auto res = writer.append(e);
                assert(res.has_value());
            }
        }
        cinder::WalReader reader(guard.path());
        for (const auto& expected : entries) {
            auto got = reader.next();
            assert(got.has_value());
            assert(*got == expected);
        }
        // No trailing garbage: the writer's bytes decode to exactly N entries.
        assert(!reader.next().has_value());

        // Group commit must be byte-identical to sequential appends, so the
        // drain fast path cannot diverge from the tested framing.
        auto batch_guard = garden.next();
        {
            cinder::WalWriter writer(batch_guard.path());
            auto res = writer.appendBatch(entries);
            assert(res.has_value());
        }
        assert(cinder_fuzz::slurp(guard.path()) == cinder_fuzz::slurp(batch_guard.path()));
    }

    void mutate(FuzzedDataProvider& fdp) {
        const size_t n = fdp.ConsumeIntegralInRange<size_t>(1, 16);
        std::vector<cinder::WalEntry> entries;
        entries.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            entries.push_back(makeEntry(fdp));
        }

        auto base = garden.next();
        {
            cinder::WalWriter writer(base.path());
            for (const auto& e : entries) {
                auto res = writer.append(e);
                assert(res.has_value());
            }
        }
        auto bytes = cinder_fuzz::slurp(base.path());
        if (bytes.empty()) {
            return;
        }

        // Flip-only and truncate-only: never insert, so records before the
        // mutation point keep identical bytes (and valid checksums). `at` stays
        // strictly inside the buffer so byte flips never write out of bounds.
        const size_t at = fdp.ConsumeIntegralInRange<size_t>(0, bytes.size() - 1);
        switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
            case 0: {
                const auto mask = (1U << fdp.ConsumeIntegralInRange<unsigned>(0, 7));
                const auto blended = static_cast<unsigned int>(bytes[at]) ^ mask;
                bytes[at] = static_cast<uint8_t>(blended);
                break;
            }
            case 1: {
                const auto blended = static_cast<unsigned int>(bytes[at])
                    ^ static_cast<unsigned int>(fdp.ConsumeIntegral<uint8_t>());
                bytes[at] = static_cast<uint8_t>(blended);
                break;
            }
            default:
                bytes.resize(at);
                break;
        }

        auto mutated = garden.next();
        cinder_fuzz::spit(mutated.path(), bytes.data(), bytes.size());
        cinder::WalReader reader(mutated.path());
        size_t returned = 0;
        while (auto got = reader.next()) {
            // Prefix oracle: every byte before the mutation is intact, so each
            // record fully preceding it checksums clean and decodes identically.
            // The first record touching the mutation fails its checksum (or hits
            // truncation) and iteration stops — never a wrong-data accept.
            if (at < K_HEADER_SIZE) {
                break; // header flip may switch legacy framing: crash-freedom only
            }
            assert(returned < entries.size());
            assert(*got == entries[returned]);
            ++returned;
        }
        assert(returned <= entries.size());
    }

    void raw(FuzzedDataProvider& fdp) {
        // Direct passthrough: arbitrary bytes (bad magic, truncations, lying
        // lengths) must never crash and never yield an invalid op.
        auto bytes = fdp.ConsumeRemainingBytes<uint8_t>();
        auto guard = garden.next();
        cinder_fuzz::spit(guard.path(), bytes.data(), bytes.size());
        cinder::WalReader reader(guard.path());
        while (auto got = reader.next()) {
            assert(got->op == cinder::WalEntry::Op::Set || got->op == cinder::WalEntry::Op::Del);
        }
    }

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
        case 0:
            roundtrip(fdp);
            break;
        case 1:
            mutate(fdp);
            break;
        default:
            raw(fdp);
            break;
    }
    return 0;
}
