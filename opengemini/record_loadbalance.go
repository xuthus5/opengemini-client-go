package opengemini

import (
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/openGemini/opengemini-client-go/proto"
)

type rpcEndpoint struct {
	ep        string
	available atomic.Bool
	conn      *grpc.ClientConn
	client    proto.WriteServiceClient
}

type rpcLoadBalance struct {
}

func newRPCLoadBalance(cfg *RPCConfig) (*rpcLoadBalance, error) {
	var eps []rpcEndpoint
	var dialOptions = []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithInitialWindowSize(1 << 24),                                    // 16MB
		grpc.WithInitialConnWindowSize(1 << 24),                                // 16MB
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 * 1024 * 1024)), // 64MB
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64 * 1024 * 1024)), // 64MB
	}
	if cfg.CompressMethod.String() != "" {
		dialOptions = append(dialOptions, grpc.WithDefaultCallOptions(grpc.UseCompressor(cfg.CompressMethod.String())))
	}
	if cfg.TlsConfig == nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		creds := credentials.NewTLS(cfg.TlsConfig)
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	}
	for _, address := range cfg.Addresses {
		addr := address.String()
		conn, err := grpc.NewClient(addr, dialOptions...)
		if err != nil {
			return nil, fmt.Errorf("connect to %s failed: %v", addr, err)
		}
		eps = append(eps, rpcEndpoint{
			ep:     addr,
			conn:   conn,
			client: proto.NewWriteServiceClient(conn)})
	}
	return &rpcLoadBalance{}, nil
}

// Next returns the next available WriteService client.
func (r *rpcLoadBalance) Next() proto.WriteServiceClient {}

func (r *rpcLoadBalance) CheckEndpoint() {
	var t = time.NewTicker(healthCheckPeriod)
	for {
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
			c.checkUpOrDown(ctx)
		}
	}
}

// Close closes the RPC load balancer and any underlying resources.
func (r *rpcLoadBalance) Close() error {}
