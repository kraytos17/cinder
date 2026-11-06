#pragma once

#include <cstddef>

namespace cinder {

    // Persistence tuning knobs shared by Config, CacheNodeServerOptions and
    // PersistenceManager::Options. One struct instead of five parallel
    // fields so the next knob is a one-line addition, not six coordinated
    // edits across config/server/wiring/tests.
    struct PersistenceTuning {
        size_t snapshot_interval_s = 60;
        size_t max_wal_entries = 10'000;
        // Rotate/compact once the WAL exceeds this many bytes (0 =
        // disabled). Entry counts alone under-trigger on large values;
        // bytes alone under-trigger on many tiny tombstones — hence both.
        // Counts sealed segments plus the active file.
        size_t max_wal_bytes = 64U << 20U;
        // Seal the active file into wal_<seq>.log once it reaches this
        // size (0 = disabled, single wal.log like before).
        size_t max_wal_segment_bytes = 16U << 20U;
        // fdatasync on every flush: acknowledged writes survive an OS
        // crash, not just a process crash. Off by default (page-cache
        // durability); enable when the WAL is the durability story.
        bool wal_fsync = false;
    };
} // namespace cinder
