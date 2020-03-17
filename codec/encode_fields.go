package codec

import (
	"fmt"
	"math"
	"reflect"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func (cb *Buffer) EncodeFieldValue(fd protoreflect.FieldDescriptor, val interface{}) error {
	if fd.IsMap() {
		mp := val.(map[interface{}]interface{})
		entryType := fd.Message()
		keyType := entryType.Fields().ByNumber(1)
		valType := entryType.Fields().ByNumber(2)
		var entryBuffer Buffer
		if cb.deterministic {
			keys := make([]interface{}, 0, len(mp))
			for k := range mp {
				keys = append(keys, k)
			}
			sort.Sort(sortable(keys))
			for _, k := range keys {
				v := mp[k]
				entryBuffer.Reset()
				if err := entryBuffer.encodeFieldElement(keyType, k); err != nil {
					return err
				}
				rv := reflect.ValueOf(v)
				if rv.Kind() != reflect.Ptr || !rv.IsNil() {
					if err := entryBuffer.encodeFieldElement(valType, v); err != nil {
						return err
					}
				}
				if err := cb.EncodeTagAndWireType(fd.Number(), WireBytes); err != nil {
					return err
				}
				if err := cb.EncodeRawBytes(entryBuffer.Bytes()); err != nil {
					return err
				}
			}
		} else {
			for k, v := range mp {
				entryBuffer.Reset()
				if err := entryBuffer.encodeFieldElement(keyType, k); err != nil {
					return err
				}
				rv := reflect.ValueOf(v)
				if rv.Kind() != reflect.Ptr || !rv.IsNil() {
					if err := entryBuffer.encodeFieldElement(valType, v); err != nil {
						return err
					}
				}
				if err := cb.EncodeTagAndWireType(fd.Number(), WireBytes); err != nil {
					return err
				}
				if err := cb.EncodeRawBytes(entryBuffer.Bytes()); err != nil {
					return err
				}
			}
		}
		return nil
	} else if fd.IsList() {
		sl := val.([]interface{})
		wt, err := getWireType(fd.Kind())
		if err != nil {
			return err
		}
		if isPacked(fd) && len(sl) > 0 &&
			(wt == WireVarint || wt == WireFixed32 || wt == WireFixed64) {
			// packed repeated field
			var packedBuffer Buffer
			for _, v := range sl {
				if err := packedBuffer.encodeFieldValue(fd, v); err != nil {
					return err
				}
			}
			if err := cb.EncodeTagAndWireType(fd.Number(), WireBytes); err != nil {
				return err
			}
			return cb.EncodeRawBytes(packedBuffer.Bytes())
		} else {
			// non-packed repeated field
			for _, v := range sl {
				if err := cb.encodeFieldElement(fd, v); err != nil {
					return err
				}
			}
			return nil
		}
	} else {
		return cb.encodeFieldElement(fd, val)
	}
}

func isPacked(fd protoreflect.FieldDescriptor) bool {
	opts := fd.Options().(*descriptorpb.FieldOptions)
	// if set, use that value
	if opts != nil && opts.Packed != nil {
		return opts.GetPacked()
	}
	// if unset: proto2 defaults to false, proto3 to true
	return fd.ParentFile().Syntax() == protoreflect.Proto3
}

// sortable is used to sort map keys. Values will be integers (int32, int64, uint32, and uint64),
// bools, or strings.
type sortable []interface{}

func (s sortable) Len() int {
	return len(s)
}

func (s sortable) Less(i, j int) bool {
	vi := s[i]
	vj := s[j]
	switch reflect.TypeOf(vi).Kind() {
	case reflect.Int32:
		return vi.(int32) < vj.(int32)
	case reflect.Int64:
		return vi.(int64) < vj.(int64)
	case reflect.Uint32:
		return vi.(uint32) < vj.(uint32)
	case reflect.Uint64:
		return vi.(uint64) < vj.(uint64)
	case reflect.String:
		return vi.(string) < vj.(string)
	case reflect.Bool:
		return !vi.(bool) && vj.(bool)
	default:
		panic(fmt.Sprintf("cannot compare keys of type %v", reflect.TypeOf(vi)))
	}
}

func (s sortable) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (b *Buffer) encodeFieldElement(fd protoreflect.FieldDescriptor, val interface{}) error {
	wt, err := getWireType(fd.Kind())
	if err != nil {
		return err
	}
	if err := b.EncodeTagAndWireType(fd.Number(), wt); err != nil {
		return err
	}
	if err := b.encodeFieldValue(fd, val); err != nil {
		return err
	}
	if wt == WireStartGroup {
		return b.EncodeTagAndWireType(fd.Number(), WireEndGroup)
	}
	return nil
}

func (b *Buffer) encodeFieldValue(fd protoreflect.FieldDescriptor, val interface{}) error {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		v := val.(bool)
		if v {
			return b.EncodeVarint(1)
		}
		return b.EncodeVarint(0)

	case protoreflect.EnumKind, protoreflect.Int32Kind:
		v := val.(int32)
		return b.EncodeVarint(uint64(v))

	case protoreflect.Sfixed32Kind:
		v := val.(int32)
		return b.EncodeFixed32(uint64(v))

	case protoreflect.Sint32Kind:
		v := val.(int32)
		return b.EncodeVarint(EncodeZigZag32(v))

	case protoreflect.Uint32Kind:
		v := val.(uint32)
		return b.EncodeVarint(uint64(v))

	case protoreflect.Fixed32Kind:
		v := val.(uint32)
		return b.EncodeFixed32(uint64(v))

	case protoreflect.Int64Kind:
		v := val.(int64)
		return b.EncodeVarint(uint64(v))

	case protoreflect.Sfixed64Kind:
		v := val.(int64)
		return b.EncodeFixed64(uint64(v))

	case protoreflect.Sint64Kind:
		v := val.(int64)
		return b.EncodeVarint(EncodeZigZag64(v))

	case protoreflect.Uint64Kind:
		v := val.(uint64)
		return b.EncodeVarint(v)

	case protoreflect.Fixed64Kind:
		v := val.(uint64)
		return b.EncodeFixed64(v)

	case protoreflect.DoubleKind:
		v := val.(float64)
		return b.EncodeFixed64(math.Float64bits(v))

	case protoreflect.FloatKind:
		v := val.(float32)
		return b.EncodeFixed32(uint64(math.Float32bits(v)))

	case protoreflect.BytesKind:
		v := val.([]byte)
		return b.EncodeRawBytes(v)

	case protoreflect.StringKind:
		v := val.(string)
		return b.EncodeRawBytes(([]byte)(v))

	case protoreflect.MessageKind:
		return b.EncodeDelimitedMessage(val.(proto.Message))

	case protoreflect.GroupKind:
		// just append the nested message to this buffer
		return b.EncodeMessage(val.(proto.Message))
		// whosoever writeth start-group tag (e.g. caller) is responsible for writing end-group tag

	default:
		return fmt.Errorf("unrecognized field type: %v", fd.Kind())
	}
}

func getWireType(t protoreflect.Kind) (int8, error) {
	switch t {
	case protoreflect.EnumKind,
		protoreflect.BoolKind,
		protoreflect.Int32Kind,
		protoreflect.Sint32Kind,
		protoreflect.Uint32Kind,
		protoreflect.Int64Kind,
		protoreflect.Sint64Kind,
		protoreflect.Uint64Kind:
		return WireVarint, nil

	case protoreflect.Fixed32Kind,
		protoreflect.Sfixed32Kind,
		protoreflect.FloatKind:
		return WireFixed32, nil

	case protoreflect.Fixed64Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.DoubleKind:
		return WireFixed64, nil

	case protoreflect.BytesKind,
		protoreflect.StringKind,
		protoreflect.MessageKind:
		return WireBytes, nil

	case protoreflect.GroupKind:
		return WireStartGroup, nil

	default:
		return 0, ErrBadWireType
	}
}
