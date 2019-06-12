// Code generated by protoc-gen-go. DO NOT EDIT.
// source: payload.proto

package internal

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Payload struct {
	Name                 string   `protobuf:"bytes,1,opt,name=Name,proto3" json:"Name,omitempty"`
	Balance              int32    `protobuf:"varint,2,opt,name=Balance,proto3" json:"Balance,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Payload) Reset()         { *m = Payload{} }
func (m *Payload) String() string { return proto.CompactTextString(m) }
func (*Payload) ProtoMessage()    {}
func (*Payload) Descriptor() ([]byte, []int) {
	return fileDescriptor_678c914f1bee6d56, []int{0}
}

func (m *Payload) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Payload.Unmarshal(m, b)
}
func (m *Payload) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Payload.Marshal(b, m, deterministic)
}
func (m *Payload) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Payload.Merge(m, src)
}
func (m *Payload) XXX_Size() int {
	return xxx_messageInfo_Payload.Size(m)
}
func (m *Payload) XXX_DiscardUnknown() {
	xxx_messageInfo_Payload.DiscardUnknown(m)
}

var xxx_messageInfo_Payload proto.InternalMessageInfo

func (m *Payload) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *Payload) GetBalance() int32 {
	if m != nil {
		return m.Balance
	}
	return 0
}

func init() {
	proto.RegisterType((*Payload)(nil), "internal.Payload")
}

func init() { proto.RegisterFile("payload.proto", fileDescriptor_678c914f1bee6d56) }

var fileDescriptor_678c914f1bee6d56 = []byte{
	// 98 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2d, 0x48, 0xac, 0xcc,
	0xc9, 0x4f, 0x4c, 0xd1, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0xc8, 0xcc, 0x2b, 0x49, 0x2d,
	0xca, 0x4b, 0xcc, 0x51, 0x32, 0xe7, 0x62, 0x0f, 0x80, 0x48, 0x09, 0x09, 0x71, 0xb1, 0xf8, 0x25,
	0xe6, 0xa6, 0x4a, 0x30, 0x2a, 0x30, 0x6a, 0x70, 0x06, 0x81, 0xd9, 0x42, 0x12, 0x5c, 0xec, 0x4e,
	0x89, 0x39, 0x89, 0x79, 0xc9, 0xa9, 0x12, 0x4c, 0x0a, 0x8c, 0x1a, 0xac, 0x41, 0x30, 0x6e, 0x12,
	0x1b, 0xd8, 0x24, 0x63, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0xea, 0x1a, 0x7a, 0x9e, 0x5a, 0x00,
	0x00, 0x00,
}
