#include <gtest/gtest.h>

TEST(ReplicaFailoverTest, NeedsMultiNode) {
    GTEST_SKIP() << "requires multi-node cluster setup";
}
