package protoparse

import (
	"bytes"
	"errors"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	"github.com/golang/protobuf/proto"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"google.golang.org/protobuf/encoding/protojson"
	newproto "google.golang.org/protobuf/proto"

	"github.com/jhump/protoreflect/codec"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/internal"
	"github.com/jhump/protoreflect/internal/testutil"
)

func TestEmptyParse(t *testing.T) {
	p := Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			return ioutil.NopCloser(bytes.NewReader(nil)), nil
		},
	}
	fd, err := p.ParseFiles("foo.proto")
	testutil.Ok(t, err)
	testutil.Eq(t, 1, len(fd))
	testutil.Eq(t, "foo.proto", fd[0].GetName())
	testutil.Eq(t, 0, len(fd[0].GetDependencies()))
	testutil.Eq(t, 0, len(fd[0].GetMessageTypes()))
	testutil.Eq(t, 0, len(fd[0].GetEnumTypes()))
	testutil.Eq(t, 0, len(fd[0].GetExtensions()))
	testutil.Eq(t, 0, len(fd[0].GetServices()))
}

func TestSimpleParse(t *testing.T) {
	protos := map[string]*parseResult{}

	// Just verify that we can successfully parse the same files we use for
	// testing. We do a *very* shallow check of what was parsed because we know
	// it won't be fully correct until after linking. (So that will be tested
	// below, where we parse *and* link.)
	res, err := parseFileForTest("../../internal/testprotos/desc_test1.proto")
	testutil.Ok(t, err)
	fd := res.fd
	testutil.Eq(t, "../../internal/testprotos/desc_test1.proto", fd.GetName())
	testutil.Eq(t, "testprotos", fd.GetPackage())
	testutil.Require(t, hasExtension(fd, "xtm"))
	testutil.Require(t, hasMessage(fd, "TestMessage"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/desc_test2.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/desc_test2.proto", fd.GetName())
	testutil.Eq(t, "testprotos", fd.GetPackage())
	testutil.Require(t, hasExtension(fd, "groupx"))
	testutil.Require(t, hasMessage(fd, "GroupX"))
	testutil.Require(t, hasMessage(fd, "Frobnitz"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/desc_test_defaults.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/desc_test_defaults.proto", fd.GetName())
	testutil.Eq(t, "testprotos", fd.GetPackage())
	testutil.Require(t, hasMessage(fd, "PrimitiveDefaults"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/desc_test_field_types.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/desc_test_field_types.proto", fd.GetName())
	testutil.Eq(t, "testprotos", fd.GetPackage())
	testutil.Require(t, hasEnum(fd, "TestEnum"))
	testutil.Require(t, hasMessage(fd, "UnaryFields"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/desc_test_options.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/desc_test_options.proto", fd.GetName())
	testutil.Eq(t, "testprotos", fd.GetPackage())
	testutil.Require(t, hasExtension(fd, "mfubar"))
	testutil.Require(t, hasEnum(fd, "ReallySimpleEnum"))
	testutil.Require(t, hasMessage(fd, "ReallySimpleMessage"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/desc_test_proto3.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/desc_test_proto3.proto", fd.GetName())
	testutil.Eq(t, "testprotos", fd.GetPackage())
	testutil.Require(t, hasEnum(fd, "Proto3Enum"))
	testutil.Require(t, hasService(fd, "TestService"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/desc_test_wellknowntypes.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/desc_test_wellknowntypes.proto", fd.GetName())
	testutil.Eq(t, "testprotos", fd.GetPackage())
	testutil.Require(t, hasMessage(fd, "TestWellKnownTypes"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/nopkg/desc_test_nopkg.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/nopkg/desc_test_nopkg.proto", fd.GetName())
	testutil.Eq(t, "", fd.GetPackage())
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/nopkg/desc_test_nopkg_new.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/nopkg/desc_test_nopkg_new.proto", fd.GetName())
	testutil.Eq(t, "", fd.GetPackage())
	testutil.Require(t, hasMessage(fd, "TopLevel"))
	protos[fd.GetName()] = res

	res, err = parseFileForTest("../../internal/testprotos/pkg/desc_test_pkg.proto")
	testutil.Ok(t, err)
	fd = res.fd
	testutil.Eq(t, "../../internal/testprotos/pkg/desc_test_pkg.proto", fd.GetName())
	testutil.Eq(t, "jhump.protoreflect.desc", fd.GetPackage())
	testutil.Require(t, hasEnum(fd, "Foo"))
	testutil.Require(t, hasMessage(fd, "Bar"))
	protos[fd.GetName()] = res

	// We'll also check our fixup logic to make sure it correctly rewrites the
	// names of the files to match corresponding import statementes. This should
	// strip the "../../internal/testprotos/" prefix from each file.
	protos = fixupFilenames(protos)
	var actual []string
	for n := range protos {
		actual = append(actual, n)
	}
	sort.Strings(actual)
	expected := []string{
		"desc_test1.proto",
		"desc_test2.proto",
		"desc_test_defaults.proto",
		"desc_test_field_types.proto",
		"desc_test_options.proto",
		"desc_test_proto3.proto",
		"desc_test_wellknowntypes.proto",
		"nopkg/desc_test_nopkg.proto",
		"nopkg/desc_test_nopkg_new.proto",
		"pkg/desc_test_pkg.proto",
	}
	testutil.Eq(t, expected, actual)
}

func parseFileForTest(filename string) (*parseResult, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	errs := newErrorHandler(nil, nil)
	res := parseProto(filename, f, errs, true)
	return res, errs.getError()
}

func hasExtension(fd *dpb.FileDescriptorProto, name string) bool {
	for _, ext := range fd.Extension {
		if ext.GetName() == name {
			return true
		}
	}
	return false
}

func hasMessage(fd *dpb.FileDescriptorProto, name string) bool {
	for _, md := range fd.MessageType {
		if md.GetName() == name {
			return true
		}
	}
	return false
}

func hasEnum(fd *dpb.FileDescriptorProto, name string) bool {
	for _, ed := range fd.EnumType {
		if ed.GetName() == name {
			return true
		}
	}
	return false
}

func hasService(fd *dpb.FileDescriptorProto, name string) bool {
	for _, sd := range fd.Service {
		if sd.GetName() == name {
			return true
		}
	}
	return false
}

func TestAggregateValueInUninterpretedOptions(t *testing.T) {
	res, err := parseFileForTest("../../internal/testprotos/desc_test_complex.proto")
	testutil.Ok(t, err)
	fd := res.fd

	aggregateValue1 := *fd.Service[0].Method[0].Options.UninterpretedOption[0].AggregateValue
	testutil.Eq(t, "{ authenticated: true permission{ action: LOGIN entity: \"client\" } }", aggregateValue1)

	aggregateValue2 := *fd.Service[0].Method[1].Options.UninterpretedOption[0].AggregateValue
	testutil.Eq(t, "{ authenticated: true permission{ action: READ entity: \"user\" } }", aggregateValue2)
}

func TestParseFilesMessageComments(t *testing.T) {
	p := Parser{
		IncludeSourceCodeInfo: true,
	}
	protos, err := p.ParseFiles("../../internal/testprotos/desc_test1.proto")
	testutil.Ok(t, err)
	comments := ""
	expected := " Comment for TestMessage\n"
	for _, p := range protos {
		msg := p.FindMessage("testprotos.TestMessage")
		if msg != nil {
			si := msg.GetSourceInfo()
			if si != nil {
				comments = si.GetLeadingComments()
			}
			break
		}
	}
	testutil.Eq(t, expected, comments)
}

func TestParseFilesWithImportsNoImportPath(t *testing.T) {
	relFilePaths := []string{
		"a/b/b1.proto",
		"a/b/b2.proto",
		"c/c.proto",
	}

	pwd, err := os.Getwd()
	testutil.Require(t, err == nil, "%v", err)

	err = os.Chdir("../../internal/testprotos/protoparse")
	testutil.Require(t, err == nil, "%v", err)
	p := Parser{}
	protos, parseErr := p.ParseFiles(relFilePaths...)
	err = os.Chdir(pwd)
	testutil.Require(t, err == nil, "%v", err)
	testutil.Require(t, parseErr == nil, "%v", parseErr)

	testutil.Ok(t, err)
	testutil.Eq(t, len(relFilePaths), len(protos))
}

func TestParseFilesWithDependencies(t *testing.T) {
	// Create some file contents that import a non-well-known proto.
	// (One of the protos in internal/testprotos is fine.)
	contents := map[string]string{
		"test.proto": `
			syntax = "proto3";
			import "desc_test_wellknowntypes.proto";

			message TestImportedType {
				testprotos.TestWellKnownTypes imported_field = 1;
			}
		`,
	}

	// Establish that we *can* parse the source file with a parser that
	// registers the dependency.
	t.Run("DependencyIncluded", func(t *testing.T) {
		// Create a dependency-aware parser.
		parser := Parser{
			Accessor: FileContentsFromMap(contents),
			LookupImport: func(imp string) (*desc.FileDescriptor, error) {
				if imp == "desc_test_wellknowntypes.proto" {
					return desc.LoadFileDescriptor(imp)
				}
				return nil, errors.New("unexpected filename")
			},
		}
		if _, err := parser.ParseFiles("test.proto"); err != nil {
			t.Errorf("Could not parse with a non-well-known import: %v", err)
		}
	})

	// Establish that we *can not* parse the source file with a parser that
	// did not register the dependency.
	t.Run("DependencyExcluded", func(t *testing.T) {
		// Create a dependency-aware parser.
		parser := Parser{
			Accessor: FileContentsFromMap(contents),
		}
		if _, err := parser.ParseFiles("test.proto"); err == nil {
			t.Errorf("Expected parse to fail due to lack of an import.")
		}
	})

	// Establish that the accessor has precedence over LookupImport.
	t.Run("AccessorWins", func(t *testing.T) {
		// Create a dependency-aware parser that should never be called.
		parser := Parser{
			Accessor: FileContentsFromMap(map[string]string{
				"test.proto": `syntax = "proto3";`,
			}),
			LookupImport: func(imp string) (*desc.FileDescriptor, error) {
				t.Errorf("LookupImport was called on a filename available to the Accessor.")
				return nil, errors.New("unimportant")
			},
		}
		if _, err := parser.ParseFiles("test.proto"); err != nil {
			t.Error(err)
		}
	})
}

func TestParseCommentsBeforeDot(t *testing.T) {
	accessor := FileContentsFromMap(map[string]string{
		"test.proto": `
syntax = "proto3";
message Foo {
  // leading comments
  .Foo foo = 1;
}
`,
	})

	p := Parser{
		Accessor:              accessor,
		IncludeSourceCodeInfo: true,
	}
	fds, err := p.ParseFiles("test.proto")
	testutil.Ok(t, err)

	comment := fds[0].GetMessageTypes()[0].GetFields()[0].GetSourceInfo().GetLeadingComments()
	testutil.Eq(t, " leading comments\n", comment)
}

func TestParseCustomOptions(t *testing.T) {
	accessor := FileContentsFromMap(map[string]string{
		"test.proto": `
syntax = "proto3";
import "google/protobuf/descriptor.proto";
extend google.protobuf.MessageOptions {
    string foo = 30303;
    int64 bar = 30304;
}
message Foo {
  option (.foo) = "foo";
  option (bar) = 123;
}
`,
	})

	p := Parser{
		Accessor:              accessor,
		IncludeSourceCodeInfo: true,
	}
	fds, err := p.ParseFiles("test.proto")
	testutil.Ok(t, err)

	md := fds[0].GetMessageTypes()[0]
	opts := md.GetMessageOptions()
	data := internal.GetUnrecognized(opts)
	buf := codec.NewBuffer(data)

	tag, wt, err := buf.DecodeTagAndWireType()
	testutil.Ok(t, err)
	testutil.Eq(t, int32(30303), tag)
	testutil.Eq(t, int8(proto.WireBytes), wt)
	fieldData, err := buf.DecodeRawBytes(false)
	testutil.Ok(t, err)
	testutil.Eq(t, "foo", string(fieldData))

	tag, wt, err = buf.DecodeTagAndWireType()
	testutil.Ok(t, err)
	testutil.Eq(t, int32(30304), tag)
	testutil.Eq(t, int8(proto.WireVarint), wt)
	fieldVal, err := buf.DecodeVarint()
	testutil.Ok(t, err)
	testutil.Eq(t, uint64(123), fieldVal)
}

func TestOptionsToJSONExperiment(t *testing.T) {
	accessor := FileContentsFromMap(map[string]string{
		"test.proto": `
syntax = "proto3";
import "google/protobuf/descriptor.proto";
extend google.protobuf.MessageOptions {
    Foo foo = 30303;
    int64 bar = 30304;
    string baz = 30305;
}
message Foo {
  option (bar) = 123;
  option (.baz) = "foo";
  option (.foo).frob = "gyzmeaux";
  option (foo).nitz.blah = 123;

  string frob = 1;
  Foo nitz = 2;
  int32 blah = 3;
}
`,
	})

	p := Parser{Accessor: accessor}
	fds, err := p.ParseFiles("test.proto")
	testutil.Ok(t, err)

	// marshal to JSON loses all custom options since they are unrecognized fields:

	fd := fds[0].AsFileDescriptorProto()
	js := protojson.Format(fd)

	t.Logf("BEFORE:\n%s", js)

	// secret sauce: convert descriptors to APIv2 ExtensionTypeResolver and re-parse unrecognized fields

	var types protoregistry.Types
	var files protoregistry.Files
	err = fileToRegistry(fds[0], &types, &files, map[*desc.FileDescriptor]struct{}{})
	testutil.Ok(t, err)
	err = reparseUnrecognized(fd.ProtoReflect(), &types)
	testutil.Ok(t, err)
	js = protojson.Format(fd)

	t.Logf("AFTER:\n%s", js)
}

func TestOptionsFromJSONExperiment(t *testing.T) {
	accessor := FileContentsFromMap(map[string]string{
		"test.proto": `
syntax = "proto3";
import "google/protobuf/descriptor.proto";
extend google.protobuf.MessageOptions {
    Foo foo = 30303;
    int64 bar = 30304;
    string baz = 30305;
}
message Foo {
  option (bar) = 123;
  option (.baz) = "foo";
  option (.foo).frob = "gyzmeaux";
  option (foo).nitz.blah = 123;

  string frob = 1;
  Foo nitz = 2;
  int32 blah = 3;
}
`,
	})

	p := Parser{Accessor: accessor}
	fds, err := p.ParseFiles("test.proto")
	testutil.Ok(t, err)

	fdSet := desc.ToFileDescriptorSet(fds...)
	var types protoregistry.Types
	var files protoregistry.Files
	err = fileToRegistry(fds[0], &types, &files, map[*desc.FileDescriptor]struct{}{})
	testutil.Ok(t, err)
	err = reparseUnrecognized(fdSet.ProtoReflect(), &types)
	testutil.Ok(t, err)
	js, err := protojson.Marshal(fdSet)
	testutil.Ok(t, err)

	// unmarshal from JSON loses all custom options since they are unrecognized fields:

	var roundTripped descriptorpb.FileDescriptorSet
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	err = opts.Unmarshal(js, &roundTripped)
	testutil.Ok(t, err)

	t.Logf("BEFORE:\n%s", prototext.Format(roundTripped.File[1]))

	// secret sauce: unfortunately, have to double-parse (no way around this if a file *uses*
	// custom options that it declares; but if it only uses custom options from dependencies,
	// we could avoid second parse if we do a streaming parse, where we build extension type
	// resolver as we go)

	types = protoregistry.Types{}
	files = protoregistry.Files{}
	for _, fd := range roundTripped.File {
		file, err := protodesc.NewFile(fd, &files)
		testutil.Ok(t, err)
		err = files.RegisterFile(file)
		testutil.Ok(t, err)
		err = contentsToRegistry(file, &types)
		testutil.Ok(t, err)
	}

	opts.Resolver = &types
	roundTripped.Reset()
	err = opts.Unmarshal(js, &roundTripped)
	testutil.Ok(t, err)

	t.Logf("AFTER:\n%s", prototext.Format(roundTripped.File[1]))
}

func fileToRegistry(fd *desc.FileDescriptor, types *protoregistry.Types, files *protoregistry.Files, seen map[*desc.FileDescriptor]struct{}) error {
	if _, ok := seen[fd]; ok {
		return nil
	}
	seen[fd] = struct{}{}
	for _, dep := range fd.GetDependencies() {
		if err := fileToRegistry(dep, types, files, seen); err != nil {
			return err
		}
	}
	refFd, err := protodesc.NewFile(fd.AsFileDescriptorProto(), files)
	if err != nil {
		return err
	}
	if err := files.RegisterFile(refFd); err != nil {
		return err
	}
	return contentsToRegistry(refFd, types)
}

type descContainer interface {
	Messages() protoreflect.MessageDescriptors
	Enums() protoreflect.EnumDescriptors
	Extensions() protoreflect.ExtensionDescriptors
}

func contentsToRegistry(d descContainer, reg *protoregistry.Types) error {
	for i := 0; i < d.Messages().Len(); i++ {
		md := d.Messages().Get(i)
		msgType := dynamicpb.NewMessageType(md)
		if err := reg.RegisterMessage(msgType); err != nil {
			return err
		}
		if err := contentsToRegistry(md, reg); err != nil {
			return err
		}
	}
	for i := 0; i < d.Enums().Len(); i++ {
		enType := dynamicpb.NewEnumType(d.Enums().Get(i))
		if err := reg.RegisterEnum(enType); err != nil {
			return err
		}
	}
	for i := 0; i < d.Extensions().Len(); i++ {
		enType := dynamicpb.NewExtensionType(d.Extensions().Get(i))
		if err := reg.RegisterExtension(enType); err != nil {
			return err
		}
	}
	return nil
}

func reparseUnrecognized(msg protoreflect.Message, reg protoregistry.ExtensionTypeResolver) error {
	unk := msg.GetUnknown()
	if len(unk) > 0 {
		msg.SetUnknown(nil)
		opts := newproto.UnmarshalOptions{Merge: true, Resolver: reg}
		if err := opts.Unmarshal(unk, msg.Interface()); err != nil {
			return err
		}
	}

	var err error
	msg.Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		err = reparseUnrecognizedInField(fd, reg, val)
		return err == nil
	})

	return err
}

func reparseUnrecognizedInField(fd protoreflect.FieldDescriptor, reg protoregistry.ExtensionTypeResolver, val protoreflect.Value) error {
	if fd.IsMap() {
		valDesc := fd.MapValue()
		if valDesc.Kind() != protoreflect.MessageKind && valDesc.Kind() != protoreflect.GroupKind {
			// nothing to reparse
			return nil
		}
		var err error
		val.Map().Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			err = reparseUnrecognized(v.Message(), reg)
			return err == nil
		})
		return err
	}

	if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
		// nothing to reparse
		return nil
	}

	if fd.IsList() {
		for i := 0; i < val.List().Len(); i++ {
			if err := reparseUnrecognized(val.List().Get(i).Message(), reg); err != nil {
				return err
			}
		}
		return nil
	}

	return reparseUnrecognized(val.Message(), reg)
}
