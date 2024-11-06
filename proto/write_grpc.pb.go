// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.3
// source: proto/write.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	WriteRowsService_WriteRows_FullMethodName = "/proto.WriteRowsService/WriteRows"
)

// WriteRowsServiceClient is the client API for WriteRowsService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type WriteRowsServiceClient interface {
	WriteRows(ctx context.Context, in *WriteRowsRequest, opts ...grpc.CallOption) (*WriteRowsResponse, error)
}

type writeRowsServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewWriteRowsServiceClient(cc grpc.ClientConnInterface) WriteRowsServiceClient {
	return &writeRowsServiceClient{cc}
}

func (c *writeRowsServiceClient) WriteRows(ctx context.Context, in *WriteRowsRequest, opts ...grpc.CallOption) (*WriteRowsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(WriteRowsResponse)
	err := c.cc.Invoke(ctx, WriteRowsService_WriteRows_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WriteRowsServiceServer is the server API for WriteRowsService service.
// All implementations must embed UnimplementedWriteRowsServiceServer
// for forward compatibility.
type WriteRowsServiceServer interface {
	WriteRows(context.Context, *WriteRowsRequest) (*WriteRowsResponse, error)
	mustEmbedUnimplementedWriteRowsServiceServer()
}

// UnimplementedWriteRowsServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedWriteRowsServiceServer struct{}

func (UnimplementedWriteRowsServiceServer) WriteRows(context.Context, *WriteRowsRequest) (*WriteRowsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteRows not implemented")
}
func (UnimplementedWriteRowsServiceServer) mustEmbedUnimplementedWriteRowsServiceServer() {}
func (UnimplementedWriteRowsServiceServer) testEmbeddedByValue()                          {}

// UnsafeWriteRowsServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to WriteRowsServiceServer will
// result in compilation errors.
type UnsafeWriteRowsServiceServer interface {
	mustEmbedUnimplementedWriteRowsServiceServer()
}

func RegisterWriteRowsServiceServer(s grpc.ServiceRegistrar, srv WriteRowsServiceServer) {
	// If the following call pancis, it indicates UnimplementedWriteRowsServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&WriteRowsService_ServiceDesc, srv)
}

func _WriteRowsService_WriteRows_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteRowsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WriteRowsServiceServer).WriteRows(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WriteRowsService_WriteRows_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WriteRowsServiceServer).WriteRows(ctx, req.(*WriteRowsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// WriteRowsService_ServiceDesc is the grpc.ServiceDesc for WriteRowsService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var WriteRowsService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.WriteRowsService",
	HandlerType: (*WriteRowsServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "WriteRows",
			Handler:    _WriteRowsService_WriteRows_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/write.proto",
}