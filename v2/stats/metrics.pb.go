// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: github.com/containerd/cgroups/v2/stats/metrics.proto

package stats

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	reflect "reflect"
	strings "strings"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type Metrics struct {
	Pids                 *PidsStat `protobuf:"bytes,1,opt,name=pids,proto3" json:"pids,omitempty"`
	CPU                  *CPUStat  `protobuf:"bytes,2,opt,name=cpu,proto3" json:"cpu,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *Metrics) Reset()      { *m = Metrics{} }
func (*Metrics) ProtoMessage() {}
func (*Metrics) Descriptor() ([]byte, []int) {
	return fileDescriptor_2fc6005842049e6b, []int{0}
}
func (m *Metrics) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Metrics) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Metrics.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Metrics) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Metrics.Merge(m, src)
}
func (m *Metrics) XXX_Size() int {
	return m.Size()
}
func (m *Metrics) XXX_DiscardUnknown() {
	xxx_messageInfo_Metrics.DiscardUnknown(m)
}

var xxx_messageInfo_Metrics proto.InternalMessageInfo

type PidsStat struct {
	Current              uint64   `protobuf:"varint,1,opt,name=current,proto3" json:"current,omitempty"`
	Limit                uint64   `protobuf:"varint,2,opt,name=limit,proto3" json:"limit,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PidsStat) Reset()      { *m = PidsStat{} }
func (*PidsStat) ProtoMessage() {}
func (*PidsStat) Descriptor() ([]byte, []int) {
	return fileDescriptor_2fc6005842049e6b, []int{1}
}
func (m *PidsStat) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PidsStat) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PidsStat.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PidsStat) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PidsStat.Merge(m, src)
}
func (m *PidsStat) XXX_Size() int {
	return m.Size()
}
func (m *PidsStat) XXX_DiscardUnknown() {
	xxx_messageInfo_PidsStat.DiscardUnknown(m)
}

var xxx_messageInfo_PidsStat proto.InternalMessageInfo

type CPUStat struct {
	Usage                *CPUUsage `protobuf:"bytes,1,opt,name=usage,proto3" json:"usage,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *CPUStat) Reset()      { *m = CPUStat{} }
func (*CPUStat) ProtoMessage() {}
func (*CPUStat) Descriptor() ([]byte, []int) {
	return fileDescriptor_2fc6005842049e6b, []int{2}
}
func (m *CPUStat) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *CPUStat) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_CPUStat.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *CPUStat) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CPUStat.Merge(m, src)
}
func (m *CPUStat) XXX_Size() int {
	return m.Size()
}
func (m *CPUStat) XXX_DiscardUnknown() {
	xxx_messageInfo_CPUStat.DiscardUnknown(m)
}

var xxx_messageInfo_CPUStat proto.InternalMessageInfo

type CPUUsage struct {
	// values in nanoseconds
	Total                uint64   `protobuf:"varint,1,opt,name=total,proto3" json:"total,omitempty"`
	Kernel               uint64   `protobuf:"varint,2,opt,name=kernel,proto3" json:"kernel,omitempty"`
	User                 uint64   `protobuf:"varint,3,opt,name=user,proto3" json:"user,omitempty"`
	PerCPU               []uint64 `protobuf:"varint,4,rep,packed,name=per_cpu,json=perCpu,proto3" json:"per_cpu,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CPUUsage) Reset()      { *m = CPUUsage{} }
func (*CPUUsage) ProtoMessage() {}
func (*CPUUsage) Descriptor() ([]byte, []int) {
	return fileDescriptor_2fc6005842049e6b, []int{3}
}
func (m *CPUUsage) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *CPUUsage) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_CPUUsage.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *CPUUsage) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CPUUsage.Merge(m, src)
}
func (m *CPUUsage) XXX_Size() int {
	return m.Size()
}
func (m *CPUUsage) XXX_DiscardUnknown() {
	xxx_messageInfo_CPUUsage.DiscardUnknown(m)
}

var xxx_messageInfo_CPUUsage proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Metrics)(nil), "io.containerd.cgroups.v2.Metrics")
	proto.RegisterType((*PidsStat)(nil), "io.containerd.cgroups.v2.PidsStat")
	proto.RegisterType((*CPUStat)(nil), "io.containerd.cgroups.v2.CPUStat")
	proto.RegisterType((*CPUUsage)(nil), "io.containerd.cgroups.v2.CPUUsage")
}

func init() {
	proto.RegisterFile("github.com/containerd/cgroups/v2/stats/metrics.proto", fileDescriptor_2fc6005842049e6b)
}

var fileDescriptor_2fc6005842049e6b = []byte{
	// 329 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x91, 0x3f, 0x4b, 0xc3, 0x40,
	0x18, 0xc6, 0x1b, 0x93, 0x26, 0xe5, 0x75, 0x3b, 0x8a, 0x04, 0x87, 0xb4, 0xc6, 0xa5, 0xd3, 0x05,
	0xaa, 0x88, 0x88, 0x53, 0x33, 0x0b, 0x21, 0x92, 0x59, 0xd2, 0xf4, 0x88, 0x87, 0x6d, 0xee, 0xbc,
	0x3f, 0x5d, 0xf5, 0xe3, 0x75, 0x74, 0x74, 0x2a, 0x36, 0x9f, 0x44, 0xee, 0x92, 0xe2, 0x54, 0xdc,
	0xde, 0xe7, 0xe5, 0xf7, 0x3c, 0xcf, 0x9b, 0x1c, 0xdc, 0xd6, 0x54, 0xbd, 0xea, 0x25, 0xae, 0xd8,
	0x26, 0xa9, 0x58, 0xa3, 0x4a, 0xda, 0x10, 0xb1, 0x4a, 0xaa, 0x5a, 0x30, 0xcd, 0x65, 0xb2, 0x9d,
	0x27, 0x52, 0x95, 0x4a, 0x26, 0x1b, 0xa2, 0x04, 0xad, 0x24, 0xe6, 0x82, 0x29, 0x86, 0x42, 0xca,
	0xf0, 0x1f, 0x8d, 0x7b, 0x1a, 0x6f, 0xe7, 0x97, 0xe3, 0x9a, 0xd5, 0xcc, 0x42, 0x89, 0x99, 0x3a,
	0x3e, 0xfe, 0x80, 0xe0, 0xa9, 0x0b, 0x40, 0x77, 0xe0, 0x71, 0xba, 0x92, 0xa1, 0x33, 0x75, 0x66,
	0xe7, 0xf3, 0x18, 0x9f, 0x4a, 0xc2, 0x19, 0x5d, 0xc9, 0x67, 0x55, 0xaa, 0xdc, 0xf2, 0xe8, 0x11,
	0xdc, 0x8a, 0xeb, 0xf0, 0xcc, 0xda, 0xae, 0x4e, 0xdb, 0xd2, 0xac, 0x30, 0xae, 0x45, 0xd0, 0xee,
	0x27, 0x6e, 0x9a, 0x15, 0xb9, 0xb1, 0xc5, 0x0f, 0x30, 0x3a, 0xe6, 0xa1, 0x10, 0x82, 0x4a, 0x0b,
	0x41, 0x1a, 0x65, 0x8f, 0xf0, 0xf2, 0xa3, 0x44, 0x63, 0x18, 0xae, 0xe9, 0x86, 0x2a, 0xdb, 0xe2,
	0xe5, 0x9d, 0x88, 0x53, 0x08, 0xfa, 0x50, 0x74, 0x0f, 0x43, 0x2d, 0xcb, 0x9a, 0xfc, 0x7f, 0x7d,
	0x9a, 0x15, 0x85, 0x21, 0xf3, 0xce, 0x10, 0xbf, 0xc3, 0xe8, 0xb8, 0x32, 0x35, 0x8a, 0xa9, 0x72,
	0xdd, 0xd7, 0x77, 0x02, 0x5d, 0x80, 0xff, 0x46, 0x44, 0x43, 0xd6, 0x7d, 0x7b, 0xaf, 0x10, 0x02,
	0x4f, 0x4b, 0x22, 0x42, 0xd7, 0x6e, 0xed, 0x8c, 0xae, 0x21, 0xe0, 0x44, 0xbc, 0x98, 0x1f, 0xe2,
	0x4d, 0xdd, 0x99, 0xb7, 0x80, 0x76, 0x3f, 0xf1, 0x33, 0x22, 0xcc, 0x07, 0xfb, 0x9c, 0x88, 0x94,
	0xeb, 0x45, 0xb8, 0x3b, 0x44, 0x83, 0xef, 0x43, 0x34, 0xf8, 0x6c, 0x23, 0x67, 0xd7, 0x46, 0xce,
	0x57, 0x1b, 0x39, 0x3f, 0x6d, 0xe4, 0x2c, 0x7d, 0xfb, 0x2a, 0x37, 0xbf, 0x01, 0x00, 0x00, 0xff,
	0xff, 0xf3, 0xea, 0x41, 0x99, 0xfd, 0x01, 0x00, 0x00,
}

func (m *Metrics) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Metrics) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Pids != nil {
		dAtA[i] = 0xa
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.Pids.Size()))
		n1, err := m.Pids.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n1
	}
	if m.CPU != nil {
		dAtA[i] = 0x12
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.CPU.Size()))
		n2, err := m.CPU.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n2
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *PidsStat) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PidsStat) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Current != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.Current))
	}
	if m.Limit != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.Limit))
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *CPUStat) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *CPUStat) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Usage != nil {
		dAtA[i] = 0xa
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.Usage.Size()))
		n3, err := m.Usage.MarshalTo(dAtA[i:])
		if err != nil {
			return 0, err
		}
		i += n3
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func (m *CPUUsage) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *CPUUsage) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Total != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.Total))
	}
	if m.Kernel != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.Kernel))
	}
	if m.User != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(m.User))
	}
	if len(m.PerCPU) > 0 {
		dAtA5 := make([]byte, len(m.PerCPU)*10)
		var j4 int
		for _, num := range m.PerCPU {
			for num >= 1<<7 {
				dAtA5[j4] = uint8(uint64(num)&0x7f | 0x80)
				num >>= 7
				j4++
			}
			dAtA5[j4] = uint8(num)
			j4++
		}
		dAtA[i] = 0x22
		i++
		i = encodeVarintMetrics(dAtA, i, uint64(j4))
		i += copy(dAtA[i:], dAtA5[:j4])
	}
	if m.XXX_unrecognized != nil {
		i += copy(dAtA[i:], m.XXX_unrecognized)
	}
	return i, nil
}

func encodeVarintMetrics(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *Metrics) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Pids != nil {
		l = m.Pids.Size()
		n += 1 + l + sovMetrics(uint64(l))
	}
	if m.CPU != nil {
		l = m.CPU.Size()
		n += 1 + l + sovMetrics(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *PidsStat) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Current != 0 {
		n += 1 + sovMetrics(uint64(m.Current))
	}
	if m.Limit != 0 {
		n += 1 + sovMetrics(uint64(m.Limit))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *CPUStat) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Usage != nil {
		l = m.Usage.Size()
		n += 1 + l + sovMetrics(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *CPUUsage) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Total != 0 {
		n += 1 + sovMetrics(uint64(m.Total))
	}
	if m.Kernel != 0 {
		n += 1 + sovMetrics(uint64(m.Kernel))
	}
	if m.User != 0 {
		n += 1 + sovMetrics(uint64(m.User))
	}
	if len(m.PerCPU) > 0 {
		l = 0
		for _, e := range m.PerCPU {
			l += sovMetrics(uint64(e))
		}
		n += 1 + sovMetrics(uint64(l)) + l
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovMetrics(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozMetrics(x uint64) (n int) {
	return sovMetrics(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (this *Metrics) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&Metrics{`,
		`Pids:` + strings.Replace(fmt.Sprintf("%v", this.Pids), "PidsStat", "PidsStat", 1) + `,`,
		`CPU:` + strings.Replace(fmt.Sprintf("%v", this.CPU), "CPUStat", "CPUStat", 1) + `,`,
		`XXX_unrecognized:` + fmt.Sprintf("%v", this.XXX_unrecognized) + `,`,
		`}`,
	}, "")
	return s
}
func (this *PidsStat) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&PidsStat{`,
		`Current:` + fmt.Sprintf("%v", this.Current) + `,`,
		`Limit:` + fmt.Sprintf("%v", this.Limit) + `,`,
		`XXX_unrecognized:` + fmt.Sprintf("%v", this.XXX_unrecognized) + `,`,
		`}`,
	}, "")
	return s
}
func (this *CPUStat) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&CPUStat{`,
		`Usage:` + strings.Replace(fmt.Sprintf("%v", this.Usage), "CPUUsage", "CPUUsage", 1) + `,`,
		`XXX_unrecognized:` + fmt.Sprintf("%v", this.XXX_unrecognized) + `,`,
		`}`,
	}, "")
	return s
}
func (this *CPUUsage) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&CPUUsage{`,
		`Total:` + fmt.Sprintf("%v", this.Total) + `,`,
		`Kernel:` + fmt.Sprintf("%v", this.Kernel) + `,`,
		`User:` + fmt.Sprintf("%v", this.User) + `,`,
		`PerCPU:` + fmt.Sprintf("%v", this.PerCPU) + `,`,
		`XXX_unrecognized:` + fmt.Sprintf("%v", this.XXX_unrecognized) + `,`,
		`}`,
	}, "")
	return s
}
func valueToStringMetrics(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("*%v", pv)
}
func (m *Metrics) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetrics
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Metrics: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Metrics: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Pids", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetrics
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMetrics
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Pids == nil {
				m.Pids = &PidsStat{}
			}
			if err := m.Pids.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CPU", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetrics
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMetrics
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.CPU == nil {
				m.CPU = &CPUStat{}
			}
			if err := m.CPU.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMetrics(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PidsStat) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetrics
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PidsStat: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PidsStat: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Current", wireType)
			}
			m.Current = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Current |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Limit", wireType)
			}
			m.Limit = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Limit |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetrics(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *CPUStat) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetrics
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: CPUStat: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: CPUStat: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Usage", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthMetrics
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthMetrics
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Usage == nil {
				m.Usage = &CPUUsage{}
			}
			if err := m.Usage.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipMetrics(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *CPUUsage) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowMetrics
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: CPUUsage: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: CPUUsage: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Total", wireType)
			}
			m.Total = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Total |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Kernel", wireType)
			}
			m.Kernel = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Kernel |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field User", wireType)
			}
			m.User = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.User |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType == 0 {
				var v uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowMetrics
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					v |= uint64(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				m.PerCPU = append(m.PerCPU, v)
			} else if wireType == 2 {
				var packedLen int
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowMetrics
					}
					if iNdEx >= l {
						return io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					packedLen |= int(b&0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				if packedLen < 0 {
					return ErrInvalidLengthMetrics
				}
				postIndex := iNdEx + packedLen
				if postIndex < 0 {
					return ErrInvalidLengthMetrics
				}
				if postIndex > l {
					return io.ErrUnexpectedEOF
				}
				var elementCount int
				var count int
				for _, integer := range dAtA[iNdEx:postIndex] {
					if integer < 128 {
						count++
					}
				}
				elementCount = count
				if elementCount != 0 && len(m.PerCPU) == 0 {
					m.PerCPU = make([]uint64, 0, elementCount)
				}
				for iNdEx < postIndex {
					var v uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowMetrics
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						v |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					m.PerCPU = append(m.PerCPU, v)
				}
			} else {
				return fmt.Errorf("proto: wrong wireType = %d for field PerCPU", wireType)
			}
		default:
			iNdEx = preIndex
			skippy, err := skipMetrics(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthMetrics
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipMetrics(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowMetrics
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowMetrics
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthMetrics
			}
			iNdEx += length
			if iNdEx < 0 {
				return 0, ErrInvalidLengthMetrics
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowMetrics
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipMetrics(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
				if iNdEx < 0 {
					return 0, ErrInvalidLengthMetrics
				}
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthMetrics = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowMetrics   = fmt.Errorf("proto: integer overflow")
)