package main

import (
	"github.com/kraytos17/cinder/internal/net/protocol"
)

type connectionOptions struct {
	host, tlsCert, tlsKey, tlsCA, clientSecret string
	port                                       int
	useTLS                                     bool
}

type adminOperation uint8

const (
	adminInfo adminOperation = iota
	adminCluster
	adminRing
	adminCompact
	adminConfigReload
	adminShutdown
)

func (op adminOperation) wireOpcode() protocol.Opcode {
	switch op {
	case adminInfo:
		return protocol.OpAdminInfo
	case adminCluster:
		return protocol.OpAdminCluster
	case adminRing:
		return protocol.OpAdminRing
	case adminCompact:
		return protocol.OpAdminCompact
	case adminConfigReload:
		return protocol.OpAdminConfigReload
	case adminShutdown:
		return protocol.OpAdminShutdown
	default:
		return 0
	}
}

func statusString(s protocol.Status) string {
	switch s {
	case protocol.StatusOK:
		return "OK"
	case protocol.StatusNotFound:
		return "(not found)"
	case protocol.StatusCapacityExceeded:
		return "(capacity exceeded)"
	case protocol.StatusInvalidArgument:
		return "(invalid argument)"
	case protocol.StatusTTLExpired:
		return "(ttl expired)"
	case protocol.StatusNotSupported:
		return "(not supported)"
	case protocol.StatusInternalError:
		return "(internal error)"
	case protocol.StatusTimeout:
		return "(timeout)"
	case protocol.StatusNotReady:
		return "(not ready)"
	case protocol.StatusPermissionDenied:
		return "(permission denied)"
	case protocol.StatusCorruptData:
		return "(corrupt data)"
	default:
		return "(unknown)"
	}
}
