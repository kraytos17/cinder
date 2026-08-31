#include <gtest/gtest.h>
#include <string>

#include "cinder/common/metrics.hpp"
#include "cinder/net/http_parser.hpp"

using namespace cinder;

TEST(MetricsTest, ZeroCounters) {
    MetricsCollector m;
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_cache_hits_total 0"), std::string::npos);
    EXPECT_NE(text.find("cinder_cache_misses_total 0"), std::string::npos);
    EXPECT_NE(text.find("cinder_store_bytes 0"), std::string::npos);
}

TEST(MetricsTest, HitMissCounters) {
    MetricsCollector m;
    m.shardMetrics().hits.fetch_add(5);
    m.shardMetrics().misses.fetch_add(3);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_cache_hits_total 5"), std::string::npos);
    EXPECT_NE(text.find("cinder_cache_misses_total 3"), std::string::npos);
}

TEST(MetricsTest, OpcodeCounters) {
    MetricsCollector m;
    m.opcodeMetrics().gets.fetch_add(10);
    m.opcodeMetrics().sets.fetch_add(2);
    m.opcodeMetrics().dels.fetch_add(1);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_operations_total{opcode=\"get\"} 10"), std::string::npos);
    EXPECT_NE(text.find("cinder_operations_total{opcode=\"set\"} 2"), std::string::npos);
    EXPECT_NE(text.find("cinder_operations_total{opcode=\"del\"} 1"), std::string::npos);
}

TEST(MetricsTest, EvictionCounters) {
    MetricsCollector m;
    m.shardMetrics().evictions_ttl.fetch_add(7);
    m.shardMetrics().evictions_capacity.fetch_add(3);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_cache_evictions_total{cause=\"ttl\"} 7"), std::string::npos);
    EXPECT_NE(text.find("cinder_cache_evictions_total{cause=\"capacity\"} 3"), std::string::npos);
}

TEST(MetricsTest, ReplicationCounters) {
    MetricsCollector m;
    m.replicationMetrics().async_writes.fetch_add(4);
    m.replicationMetrics().quorum_writes_ok.fetch_add(2);
    m.replicationMetrics().quorum_writes_failed.fetch_add(1);
    m.replicationMetrics().read_repairs.fetch_add(5);
    m.replicationMetrics().hints_enqueued.fetch_add(3);
    m.replicationMetrics().hints_replayed.fetch_add(2);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_replication_async_writes_total 4"), std::string::npos);
    EXPECT_NE(text.find("cinder_replication_quorum_writes_ok_total 2"), std::string::npos);
    EXPECT_NE(text.find("cinder_replication_quorum_writes_failed_total 1"), std::string::npos);
    EXPECT_NE(text.find("cinder_replication_read_repairs_total 5"), std::string::npos);
    EXPECT_NE(text.find("cinder_replication_hints_enqueued_total 3"), std::string::npos);
    EXPECT_NE(text.find("cinder_replication_hints_replayed_total 2"), std::string::npos);
}

TEST(MetricsTest, ClusterCounters) {
    MetricsCollector m;
    m.clusterMetrics().gossip_rounds.fetch_add(10);
    m.clusterMetrics().probe_sent.fetch_add(5);
    m.clusterMetrics().probe_received.fetch_add(4);
    m.clusterMetrics().suspect_marked.fetch_add(1);
    m.clusterMetrics().dead_marked.fetch_add(1);
    m.clusterMetrics().rebalance_copies.fetch_add(3);
    m.clusterMetrics().rebalance_migrations.fetch_add(1);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_cluster_gossip_rounds_total 10"), std::string::npos);
    EXPECT_NE(text.find("cinder_cluster_probe_sent_total 5"), std::string::npos);
    EXPECT_NE(text.find("cinder_cluster_probe_received_total 4"), std::string::npos);
    EXPECT_NE(text.find("cinder_cluster_suspect_marked_total 1"), std::string::npos);
    EXPECT_NE(text.find("cinder_cluster_dead_marked_total 1"), std::string::npos);
    EXPECT_NE(text.find("cinder_cluster_rebalance_copies_total 3"), std::string::npos);
    EXPECT_NE(text.find("cinder_cluster_rebalance_migrations_total 1"), std::string::npos);
}

TEST(MetricsTest, ConnectionCounters) {
    MetricsCollector m;
    m.connectionMetrics().connections_opened.fetch_add(8);
    m.connectionMetrics().connections_closed.fetch_add(5);
    m.connectionMetrics().decode_failures.fetch_add(2);
    m.connectionMetrics().redirects.fetch_add(3);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_connections_opened_total 8"), std::string::npos);
    EXPECT_NE(text.find("cinder_connections_closed_total 5"), std::string::npos);
    EXPECT_NE(text.find("cinder_connections_decode_failures_total 2"), std::string::npos);
    EXPECT_NE(text.find("cinder_connections_redirects_total 3"), std::string::npos);
}

TEST(MetricsTest, ReplicaLag) {
    MetricsCollector m;
    ReplicationMetrics::ReplicaLag lag;
    lag.node_id = "node-1";
    lag.last_ack_epoch_ms = 1'000;
    m.replicationMetrics().replica_lags.push_back(lag);
    auto text = m.formatPrometheus();
    EXPECT_NE(text.find("cinder_replication_lag_ms{replica=\"node-1\"}"), std::string::npos);
}

TEST(MetricsTest, StoreGauges) {
    MetricsCollector m;
    m.shardMetrics().current_bytes.store(1'024);
    m.shardMetrics().current_entries.store(42);
    m.shardMetrics().capacity_bytes.store(1'048'576);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("cinder_store_bytes 1024"), std::string::npos);
    EXPECT_NE(text.find("cinder_store_entries 42"), std::string::npos);
    EXPECT_NE(text.find("cinder_store_capacity_bytes 1048576"), std::string::npos);
}

TEST(MetricsTest, PrometheusFormatStructure) {
    MetricsCollector m;
    m.shardMetrics().hits.fetch_add(1);
    auto text = m.formatPrometheus();

    EXPECT_NE(text.find("# HELP cinder_cache_hits_total"), std::string::npos);
    EXPECT_NE(text.find("# TYPE cinder_cache_hits_total counter"), std::string::npos);
}

TEST(MetricsTest, CopyMoveDeleted) {
    static_assert(!std::is_copy_constructible_v<MetricsCollector>);
    static_assert(!std::is_copy_assignable_v<MetricsCollector>);
    static_assert(!std::is_move_constructible_v<MetricsCollector>);
    static_assert(!std::is_move_assignable_v<MetricsCollector>);
}

TEST(MetricsTest, LatencyHistogramPercentile) {
    LatencyHistogram h;
    // 100 samples spread across two buckets (~1µs and ~10µs).
    for (int i = 0; i < 50; ++i) {
        h.record(1'000);
    }
    for (int i = 0; i < 50; ++i) {
        h.record(10'000);
    }

    EXPECT_EQ(h.count.load(), 100);
    EXPECT_EQ(h.total_ns.load(), 50 * 1'000 + 50 * 10'000);
    EXPECT_GE(h.percentile(0.50), 1'000);
    EXPECT_LE(h.percentile(0.50), 20'000);
    EXPECT_GE(h.percentile(0.99), 8'192); // bucket upper bound for 10µs samples
    EXPECT_LE(h.percentile(0.99), 20'000);
}

TEST(MetricsTest, LatencyHistogramEmpty) {
    LatencyHistogram h;
    EXPECT_EQ(h.count.load(), 0);
    EXPECT_EQ(h.percentile(0.99), 0);
}

TEST(MetricsTest, LatencyHistogramClampOversize) {
    LatencyHistogram h;
    h.record(1); // minimum
    EXPECT_GE(h.percentile(0.5), 1);
    h.record(uint64_t{1} << 60U); // beyond the last bucket
    EXPECT_EQ(h.percentile(1.0), ~uint64_t{0});
}

TEST(MetricsTest, LatencyExport) {
    MetricsCollector m;
    m.opcodeMetrics().recordLatency(1, 2'500); // get
    m.opcodeMetrics().recordLatency(1, 7'500); // get
    m.opcodeMetrics().recordLatency(2, 4'000); // set
    auto text = m.formatPrometheus();

    EXPECT_NE(
        text.find("cinder_request_latency_seconds_count{opcode=\"get\"} 2"), std::string::npos);
    EXPECT_NE(text.find("cinder_request_latency_seconds_sum{opcode=\"get\"}"), std::string::npos);
    EXPECT_NE(text.find("cinder_request_latency_seconds{opcode=\"get\",quantile=\"0.5\"}"),
        std::string::npos);
    EXPECT_NE(text.find("cinder_request_latency_seconds{opcode=\"get\",quantile=\"0.99\"}"),
        std::string::npos);
    EXPECT_NE(
        text.find("cinder_request_latency_seconds_count{opcode=\"set\"} 1"), std::string::npos);
    // Unused opcode (e.g. del) must not appear in the latency summary.
    EXPECT_EQ(text.find("cinder_request_latency_seconds{opcode=\"del\""), std::string::npos);
}

TEST(HttpParserTest, ValidGet) {
    auto req = cinder::net::parseHttpRequest("GET /metrics HTTP/1.1\r\nHost: localhost\r\n");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->method, "GET");
    EXPECT_EQ(req->path, "/metrics");
    EXPECT_EQ(req->version, "HTTP/1.1");
}

TEST(HttpParserTest, InvalidMethod) {
    auto req = cinder::net::parseHttpRequest("POST /metrics HTTP/1.1\r\n");
    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->method, "POST");
}

TEST(HttpParserTest, MalformedNoSpace) {
    auto req = cinder::net::parseHttpRequest("GET");
    EXPECT_FALSE(req.has_value());
}

TEST(HttpParserTest, MalformedNoPath) {
    auto req = cinder::net::parseHttpRequest("GET HTTP/1.1\r\n");
    EXPECT_FALSE(req.has_value());
}

TEST(HttpParserTest, MalformedNoCRLF) {
    auto req = cinder::net::parseHttpRequest("GET /metrics HTTP/1.1");
    EXPECT_FALSE(req.has_value());
}

TEST(HttpParserTest, EmptyInput) {
    auto req = cinder::net::parseHttpRequest("");
    EXPECT_FALSE(req.has_value());
}

TEST(HttpParserTest, Format200) {
    auto resp = cinder::net::formatHttpResponse("test body");
    EXPECT_NE(resp.find("HTTP/1.0 200 OK"), std::string::npos);
    EXPECT_NE(resp.find("Content-Length: 9"), std::string::npos);
    EXPECT_NE(resp.find("test body"), std::string::npos);
}

TEST(HttpParserTest, Format404) {
    auto resp = cinder::net::formatHttp404();
    EXPECT_NE(resp.find("HTTP/1.0 404 Not Found"), std::string::npos);
    EXPECT_NE(resp.find("404 Not Found"), std::string::npos);
}

TEST(HttpParserTest, EmptyBody) {
    auto resp = cinder::net::formatHttpResponse("");
    EXPECT_NE(resp.find("Content-Length: 0"), std::string::npos);
}

TEST(HttpParserTest, MetricsEndpoint) {
    MetricsCollector m;
    m.shardMetrics().hits.fetch_add(42);
    auto body = m.formatPrometheus();
    auto resp = cinder::net::formatHttpResponse(body);

    EXPECT_NE(resp.find("HTTP/1.0 200 OK"), std::string::npos);
    EXPECT_NE(resp.find("cinder_cache_hits_total 42"), std::string::npos);
    EXPECT_NE(resp.find("text/plain"), std::string::npos);
}

TEST(HttpParserTest, FullGetMetricsRequest) {
    std::string request = "GET /metrics HTTP/1.0\r\nHost: localhost:9100\r\n\r\n";
    auto req = cinder::net::parseHttpRequest(request);

    ASSERT_TRUE(req.has_value());
    EXPECT_EQ(req->method, "GET");
    EXPECT_EQ(req->path, "/metrics");
    if (req->method == "GET" && req->path == "/metrics") {
        MetricsCollector m;
        auto resp = cinder::net::formatHttpResponse(m.formatPrometheus());
        EXPECT_NE(resp.find("200 OK"), std::string::npos);
    }
}
