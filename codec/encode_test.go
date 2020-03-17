package codec_test

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/jhump/protoreflect/codec"
	"github.com/jhump/protoreflect/internal/testprotos"
	"github.com/jhump/protoreflect/internal/testutil"
)

func TestEncodeMessage(t *testing.T) {
	// A generated message will be encoded using its XXX_Size and XXX_Marshal
	// methods
	pm := &testprotos.Test{
		Foo:   proto.String("bar"),
		Array: []int32{0, 1, 2, 3},
		S: &testprotos.Simple{
			Name: proto.String("baz"),
			Id:   proto.Uint64(12345),
		},
		M: map[string]int32{
			"a": 1,
			"b": 2,
			"c": 3,
			"d": 4,
		},
		B: []byte{3, 2, 1, 0},
	}

	md := pm.ProtoReflect().Descriptor()
	dm := dynamicpb.NewMessage(md)
	pm.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		setDynamicField(dm, fd, v)
		return true
	})

	testCases := []struct {
		Name string
		Msg  proto.Message
	}{
		{Name: "generated", Msg: pm},
		{Name: "dynamic", Msg: dm},
	}
	dels := []struct {
		Name      string
		Delimited bool
	}{
		{Name: "not delimited", Delimited: false},
		{Name: "delimited", Delimited: true},
	}

	var bytes []byte

	for _, dl := range dels {
		t.Run(dl.Name, func(t *testing.T) {
			t.Run("deterministic", func(t *testing.T) {
				for _, tc := range testCases {
					t.Run(tc.Name, func(t *testing.T) {
						var cb codec.Buffer
						cb.SetDeterministic(true)
						if dl.Delimited {
							err := cb.EncodeDelimitedMessage(tc.Msg)
							testutil.Ok(t, err)
						} else {
							err := cb.EncodeMessage(tc.Msg)
							testutil.Ok(t, err)
						}
						b := cb.Bytes()
						if bytes == nil {
							bytes = b
						} else if dl.Delimited {
							// delimited writes have varint-encoded length prefix
							var lenBuf codec.Buffer
							err := lenBuf.EncodeVarint(uint64(len(bytes)))
							testutil.Ok(t, err)
							testutil.Eq(t, append(lenBuf.Bytes(), bytes...), b)
						} else {
							// The generated proto message is the benchmark.
							// Ensure that the others match its output.
							testutil.Eq(t, bytes, b)
						}
					})
				}
			})

			t.Run("non-deterministic", func(t *testing.T) {
				for _, tc := range testCases {
					t.Run(tc.Name, func(t *testing.T) {
						var cb codec.Buffer
						if dl.Delimited {
							err := cb.EncodeDelimitedMessage(tc.Msg)
							testutil.Ok(t, err)
						} else {
							err := cb.EncodeMessage(tc.Msg)
							testutil.Ok(t, err)
						}

						var b []byte
						if dl.Delimited {
							// delimited writes have varint-encoded length prefix
							l, err := cb.DecodeVarint()
							testutil.Ok(t, err)
							b = cb.Bytes()
							testutil.Eq(t, int(l), len(b))
						} else {
							b = cb.Bytes()
						}
						// we can't compare byte slices to benchmark since the
						// message contains a map and we are not using deterministic
						// marshal method; so verify that unmarshaling the bytes
						// results in an equal message as the original
						var pm2 testprotos.Test
						err := proto.Unmarshal(b, &pm2)
						testutil.Ok(t, err)

						testutil.Require(t, proto.Equal(pm, &pm2))
					})
				}
			})
		})
	}
}

// NB: other field types are well-exercised by dynamic.Message serialization tests
// So we focus on serialization of groups and the various kinds of proto.Message
// implementations that can back them (similar to TestEncodeMessage above).
func TestEncodeFieldValue_Group(t *testing.T) {
	atmMd := (&testprotos.AnotherTestMessage{}).ProtoReflect().Descriptor()
	rrFd := atmMd.Fields().ByNumber(6) // tag 6 is the group

	// A generated message will be encoded using its XXX_Size and XXX_Marshal
	// methods
	pm := &testprotos.AnotherTestMessage_RockNRoll{
		Beatles: proto.String("Sgt. Pepper's Lonely Hearts Club Band"),
		Stones:  proto.String("Exile on Main St."),
		Doors:   proto.String("Strange Days"),
	}

	md := pm.ProtoReflect().Descriptor()
	dm := dynamicpb.NewMessage(md)
	pm.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		setDynamicField(dm, fd, v)
		return true
	})

	testCases := []struct {
		Name string
		Msg  proto.Message
	}{
		{Name: "generated", Msg: pm},
		{Name: "dynamic", Msg: dm},
	}

	dets := []struct {
		Name          string
		Deterministic bool
	}{
		{Name: "deterministic", Deterministic: true},
		{Name: "non-deterministic", Deterministic: false},
	}

	var bytes []byte

	for _, det := range dets {
		t.Run(det.Name, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.Name, func(t *testing.T) {
					var cb codec.Buffer
					cb.SetDeterministic(det.Deterministic)
					err := cb.EncodeFieldValue(rrFd, tc.Msg)
					testutil.Ok(t, err)
					b := cb.Bytes()
					if bytes == nil {
						bytes = b
						// make sure that the bytes are valid
						expected := &testprotos.AnotherTestMessage{Rocknroll: pm}
						var actual testprotos.AnotherTestMessage
						err := proto.Unmarshal(b, &actual)
						testutil.Ok(t, err)
						testutil.Require(t, proto.Equal(expected, &actual))
					} else {
						// The generated proto message is the benchmark.
						// Ensure that all others match its output.
						// (We can do this even for non-deterministic
						// method because the actual data being marshaled
						// has no map values, so will always be the same)
						testutil.Eq(t, bytes, b)
					}
				})
			}
		})
	}
}

func setDynamicField(dm *dynamicpb.Message, fd protoreflect.FieldDescriptor, v protoreflect.Value) {
	if fd.IsList() {
		dm.Clear(fd)
		lv := v.List()
		mut := dm.Mutable(fd).List()
		for i := 0; i < lv.Len(); i++ {
			mut.Append(lv.Get(i))
		}
	} else if fd.IsMap() {
		dm.Clear(fd)
		mv := v.Map()
		mut := dm.Mutable(fd).Map()
		mv.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			mut.Set(k, v)
			return true
		})
	} else {
		dm.Set(fd, v)
	}
}