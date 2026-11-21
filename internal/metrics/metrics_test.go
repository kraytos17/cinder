package metrics

import (
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/kraytos17/cinder/internal/net/protocol"
)

func TestOpNames(t *testing.T) {
	cases := map[protocol.Opcode]string{
		protocol.OpGet: "get", protocol.OpSet: "set", protocol.OpDel: "del",
		protocol.OpPing: "ping", protocol.OpGossip: "gossip",
		protocol.OpReplicate: "replicate", protocol.OpHint: "hint",
		protocol.OpGetVersioned:       "get_versioned",
		protocol.OpAntiEntropyDigest:  "anti_entropy_digest",
		protocol.OpAntiEntropySync:    "anti_entropy_sync",
		protocol.OpAdminInfo:          "admin_info",
		protocol.OpAdminCluster:       "admin_cluster",
		protocol.OpAdminRing:          "admin_ring",
		protocol.OpAdminCompact:       "admin_compact",
		protocol.OpAdminConfigReload:  "admin_config_reload",
		protocol.OpAdminShutdown:      "admin_shutdown",
		protocol.OpAdminDrain:         "admin_drain",
		protocol.OpCounterAdjust:      "counter_adjust",
		protocol.OpCounterMerge:       "counter_merge",
		protocol.OpCounterGet:         "counter_get",
		protocol.OpCounterAntiEntropy: "counter_anti_entropy",
	}
	for op, want := range cases {
		if got := OpName(op); got != want {
			t.Fatalf("op %d = %q, want %q", op, got, want)
		}
	}
}

// TestMetricsDocumented binds the exposition registry to docs/METRICS.md.
// The generated names are the schema surface: adding, renaming, or removing
// a counter/gauge/summary without updating the catalog (or deleting a
// documented metric without removing it from the exporter) fails this test.
func TestMetricsDocumented(t *testing.T) {
	doc, err := os.ReadFile("../../docs/METRICS.md")
	if err != nil {
		t.Fatal(err)
	}

	nameRE := regexp.MustCompile("`(cinder_[a-zA-Z0-9_]+)`")
	docNames := map[string]bool{}
	for _, match := range nameRE.FindAllSubmatch(doc, -1) {
		docNames[string(match[1])] = true
	}

	c := NewCollector()
	// The summary family is lazy; record every opcode so all documented
	// latency series appear in the rendered exposition.
	for op := protocol.Opcode(1); op <= maxOpcode; op++ {
		c.Record(op, 1000)
	}
	exposition := string(c.FormatPrometheus(StoreGauges{}))
	actualNames := map[string]bool{}
	for line := range strings.SplitSeq(exposition, "\n") {
		if !strings.HasPrefix(line, "cinder_") {
			continue
		}
		field := strings.Fields(line)
		if len(field) == 0 {
			continue
		}
		name, _, _ := strings.Cut(field[0], "{")
		actualNames[name] = true
	}

	var undocumented, stale []string
	for name := range actualNames {
		if !docNames[name] {
			undocumented = append(undocumented, name)
		}
	}
	for name := range docNames {
		if !actualNames[name] {
			stale = append(stale, name)
		}
	}
	slices.Sort(undocumented)
	slices.Sort(stale)
	if len(undocumented) != 0 || len(stale) != 0 {
		t.Fatalf("metrics catalog drift: undocumented=%v stale=%v", undocumented, stale)
	}
}

func TestHistogramPercentile(t *testing.T) {
	var h Histogram
	if got := h.Percentile(0.99); got != 0 {
		t.Fatalf("empty = %d, want 0", got)
	}
	// 100 samples at exactly 1000ns: bucket idx = floor(log2(1000)) = 9
	// ([512,1024)), upper bound 1024.
	for range 100 {
		h.Record(1000)
	}
	if got := h.Percentile(0.5); got != 1024 {
		t.Fatalf("p50 = %d, want 1024", got)
	}
	if got := h.Percentile(0.999); got != 1024 {
		t.Fatalf("p999 = %d, want 1024", got)
	}
	if got := h.Count(); got != 100 {
		t.Fatalf("count = %d, want 100", got)
	}
	if got := h.SumNs(); got != 100_000 {
		t.Fatalf("sum = %d, want 100000", got)
	}
	// Zero records land in bucket 0 (upper bound 2).
	var z Histogram
	z.Record(0)
	if got := z.Percentile(0.5); got != 2 {
		t.Fatalf("zero p50 = %d, want 2", got)
	}
}

func TestPrometheusFormat(t *testing.T) {
	c := NewCollector()
	c.Record(protocol.OpGet, 1000)
	c.Record(protocol.OpGet, 2000)
	c.Record(protocol.OpCounterAdjust, 500)
	c.Hits.Add(3)
	c.Misses.Add(1)
	out := string(c.FormatPrometheus(StoreGauges{Bytes: 10, Entries: 2, Capacity: 100}))
	for _, want := range []string{
		"# HELP cinder_cache_hits_total",
		"# TYPE cinder_cache_hits_total counter\ncinder_cache_hits_total 3\n",
		"cinder_operations_total{opcode=\"get\"} 2\n",
		"cinder_operations_total{opcode=\"counter_adjust\"} 1\n",
		"cinder_request_latency_seconds_count{opcode=\"get\"} 2\n",
		"cinder_request_latency_seconds{opcode=\"get\",quantile=\"0.5\"}",
		"cinder_store_bytes 10\n",
		"cinder_cluster_gossip_rounds_total 0\n",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestHandlerServesMetrics(t *testing.T) {
	c := NewCollector()
	c.Record(protocol.OpPing, 10)
	h := NewHandler(c, func() StoreGauges { return StoreGauges{Bytes: 7} })
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type = %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "cinder_operations_total") {
		t.Fatal("no operations in body")
	}
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/other", nil))
	if rec2.Code != 404 {
		t.Fatalf("other path code = %d, want 404", rec2.Code)
	}
}
