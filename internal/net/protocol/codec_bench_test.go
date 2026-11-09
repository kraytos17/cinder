package protocol

import (
	"testing"
)

func benchRequest() Request {
	return Request{
		Opcode: OpSet, Key: []byte("bench-key-0001"), Value: make([]byte, 256),
		Version: 42, WriterNodeHash: 7,
	}
}

func benchResponse() Response {
	return Response{Status: StatusOK, Value: make([]byte, 256), HasValue: true, Version: 42}
}

// BenchmarkEncodeRequest measures frame serialization of a 256 B set.
func BenchmarkEncodeRequest(b *testing.B) {
	req := benchRequest()
	b.SetBytes(int64(len(req.Key) + len(req.Value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeRequest(req, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeRequest measures the parse path over a fixed frame.
func BenchmarkDecodeRequest(b *testing.B) {
	raw, err := EncodeRequest(benchRequest(), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeRequest(raw); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEncodeResponse / BenchmarkDecodeResponse cover the reply path.
func BenchmarkEncodeResponse(b *testing.B) {
	res := benchResponse()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeResponse(res); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeResponse(b *testing.B) {
	raw, err := EncodeResponse(benchResponse())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeResponse(raw); err != nil {
			b.Fatal(err)
		}
	}
}
