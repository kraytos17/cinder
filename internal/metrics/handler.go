package metrics

import (
	"net/http"
)

// Handler serves the Prometheus exposition at GET /metrics. It reads live
// store gauges through the callback on each scrape, avoiding duplicated
// gauge state that could drift from the store.
type Handler struct {
	c      *Collector
	gauges func() StoreGauges
}

// NewHandler returns an http.Handler over c.
func NewHandler(c *Collector, gauges func() StoreGauges) http.Handler {
	if gauges == nil {
		gauges = func() StoreGauges { return StoreGauges{} }
	}
	return &Handler{c: c, gauges: gauges}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(h.c.FormatPrometheus(h.gauges()))
}
