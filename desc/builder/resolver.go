package builder

import (
	"fmt"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/dynamicpb"
	"reflect"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/jhump/protoreflect/desc"
)

type dependencies struct {
	descs map[protoreflect.FileDescriptor]struct{}
	exts  protoregistry.Types
}

func newDependencies() *dependencies {
	return &dependencies{
		descs: map[protoreflect.FileDescriptor]struct{}{},
	}
}

func (d *dependencies) add(fd protoreflect.FileDescriptor) {
	if _, ok := d.descs[fd]; ok {
		// already added
		return
	}
	d.descs[fd] = struct{}{}
	addExts(fd, d.exts)
}

func addExts(ns namespace, reg protoregistry.Types) {
	exts := ns.Extensions()
	for i := 0; i < exts.Len(); i++ {
		_ = reg.RegisterExtension(dynamicpb.NewExtensionType(exts.Get(i)))
	}
	msgs := ns.Messages()
	for i := 0; i < msgs.Len(); i++ {
		addExts(msgs.Get(i), reg)
	}
}

// dependencyResolver is the work-horse for converting a tree of builders into a
// tree of descriptors. It scans a root (usually a file builder) and recursively
// resolves all dependencies (references to builders in other trees as well as
// references to other already-built descriptors). The result of resolution is a
// file descriptor (or an error).
type dependencyResolver struct {
	resolvedRoots map[Builder]protoreflect.FileDescriptor
	seen          map[Builder]struct{}
	opts          BuilderOptions
}

func newResolver(opts BuilderOptions) *dependencyResolver {
	return &dependencyResolver{
		resolvedRoots: map[Builder]protoreflect.FileDescriptor{},
		seen:          map[Builder]struct{}{},
		opts:          opts,
	}
}

func (r *dependencyResolver) resolveElement(b Builder, seen []Builder) (protoreflect.FileDescriptor, error) {
	b = getRoot(b)

	if fd, ok := r.resolvedRoots[b]; ok {
		return fd, nil
	}

	for _, s := range seen {
		if s == b {
			names := make([]string, len(seen)+1)
			for i, s := range seen {
				names[i] = s.GetName()
			}
			names[len(seen)] = b.GetName()
			return nil, fmt.Errorf("descriptors have cyclic dependency: %s", strings.Join(names, " ->  "))
		}
	}
	seen = append(seen, b)

	var fd protoreflect.FileDescriptor
	var err error
	switch b := b.(type) {
	case *FileBuilder:
		fd, err = r.resolveFile(b, b, seen)
	default:
		fd, err = r.resolveSyntheticFile(b, seen)
	}
	if err != nil {
		return nil, err
	}
	r.resolvedRoots[b] = fd
	return fd, nil
}

func (r *dependencyResolver) resolveFile(fb *FileBuilder, root Builder, seen []Builder) (protoreflect.FileDescriptor, error) {
	deps := newDependencies()
	// add explicit imports first
	for fd := range fb.explicitImports {
		deps.add(fd)
	}
	for dep := range fb.explicitDeps {
		if dep == fb {
			// ignore erroneous self references
			continue
		}
		fd, err := r.resolveElement(dep, seen)
		if err != nil {
			return nil, err
		}
		deps.add(fd)
	}
	// now accumulate implicit dependencies based on other types referenced
	for _, mb := range fb.messages {
		if err := r.resolveTypesInMessage(root, seen, deps, mb); err != nil {
			return nil, err
		}
	}
	for _, exb := range fb.extensions {
		if err := r.resolveTypesInExtension(root, seen, deps, exb); err != nil {
			return nil, err
		}
	}
	for _, sb := range fb.services {
		if err := r.resolveTypesInService(root, seen, deps, sb); err != nil {
			return nil, err
		}
	}

	// finally, resolve custom options (which may refer to deps already
	// computed above)
	if err := r.resolveTypesInFileOptions(root, deps, fb); err != nil {
		return nil, err
	}

	depSlice := make([]protoreflect.FileDescriptor, 0, len(deps.descs))
	for dep := range deps.descs {
		depSlice = append(depSlice, dep)
	}

	fp, err := fb.buildProto(depSlice)
	if err != nil {
		return nil, err
	}

	// make sure this file name doesn't collide with any of its dependencies
	fileNames := map[protoreflect.Name]struct{}{}
	for _, d := range depSlice {
		addFileNames(d, fileNames)
		fileNames[d.Name()] = struct{}{}
	}
	unique := makeUnique(fp.GetName(), fileNames)
	if unique != fp.GetName() {
		fp.Name = proto.String(unique)
	}

	// TODO: protoregistry.Files enforces harder constraints than our v1
	// implementation in desc... should we keep desc package as our own
	// way to turn protos into descriptors, just for softer constraints?
	var files protoregistry.Files
	for _, dep := range depSlice {
		if err := files.RegisterFile(dep); err != nil {
			return nil, err
		}
	}

	return protodesc.NewFile(fp, &files)
}

func addFileNames(fd protoreflect.FileDescriptor, files map[protoreflect.Name]struct{}) {
	if _, ok := files[fd.Name()]; ok {
		// already added
		return
	}
	files[fd.Name()] = struct{}{}
	deps := fd.Imports()
	for i := 0; i < deps.Len(); i++ {
		addFileNames(deps.Get(i).FileDescriptor, files)
	}
}

func (r *dependencyResolver) resolveSyntheticFile(b Builder, seen []Builder) (protoreflect.FileDescriptor, error) {
	// find ancestor to temporarily attach to new file
	curr := b
	for curr.GetParent() != nil {
		curr = curr.GetParent()
	}
	f := NewFile("")
	switch curr := curr.(type) {
	case *MessageBuilder:
		f.messages = append(f.messages, curr)
	case *EnumBuilder:
		f.enums = append(f.enums, curr)
	case *ServiceBuilder:
		f.services = append(f.services, curr)
	case *FieldBuilder:
		if curr.IsExtension() {
			f.extensions = append(f.extensions, curr)
		} else {
			panic("field must be added to message before calling Build()")
		}
	case *OneOfBuilder:
		if _, ok := b.(*OneOfBuilder); ok {
			panic("one-of must be added to message before calling Build()")
		} else {
			// b was a child of one-of which means it must have been a field
			panic("field must be added to message before calling Build()")
		}
	case *MethodBuilder:
		panic("method must be added to service before calling Build()")
	case *EnumValueBuilder:
		panic("enum value must be added to enum before calling Build()")
	default:
		panic(fmt.Sprintf("Unrecognized kind of builder: %T", b))
	}
	curr.setParent(f)

	// don't forget to reset when done
	defer func() {
		curr.setParent(nil)
	}()

	return r.resolveFile(f, b, seen)
}

func (r *dependencyResolver) resolveTypesInMessage(root Builder, seen []Builder, deps *dependencies, mb *MessageBuilder) error {
	for _, b := range mb.fieldsAndOneOfs {
		if flb, ok := b.(*FieldBuilder); ok {
			if err := r.resolveTypesInField(root, seen, deps, flb); err != nil {
				return err
			}
		} else {
			oob := b.(*OneOfBuilder)
			for _, flb := range oob.choices {
				if err := r.resolveTypesInField(root, seen, deps, flb); err != nil {
					return err
				}
			}
		}
	}
	for _, nmb := range mb.nestedMessages {
		if err := r.resolveTypesInMessage(root, seen, deps, nmb); err != nil {
			return err
		}
	}
	for _, exb := range mb.nestedExtensions {
		if err := r.resolveTypesInExtension(root, seen, deps, exb); err != nil {
			return err
		}
	}
	return nil
}

func (r *dependencyResolver) resolveTypesInExtension(root Builder, seen []Builder, deps *dependencies, exb *FieldBuilder) error {
	if err := r.resolveTypesInField(root, seen, deps, exb); err != nil {
		return err
	}
	if exb.foreignExtendee != nil {
		deps.add(exb.foreignExtendee.ParentFile())
	} else if err := r.resolveType(root, seen, exb.localExtendee, deps); err != nil {
		return err
	}
	return nil
}

func (r *dependencyResolver) resolveTypesInService(root Builder, seen []Builder, deps *dependencies, sb *ServiceBuilder) error {
	for _, mtb := range sb.methods {
		if err := r.resolveRpcType(root, seen, mtb.ReqType, deps); err != nil {
			return err
		}
		if err := r.resolveRpcType(root, seen, mtb.RespType, deps); err != nil {
			return err
		}
	}
	return nil
}

func (r *dependencyResolver) resolveRpcType(root Builder, seen []Builder, t *RpcType, deps *dependencies) error {
	if t.foreignType != nil {
		deps.add(t.foreignType.ParentFile())
	} else {
		return r.resolveType(root, seen, t.localType, deps)
	}
	return nil
}

func (r *dependencyResolver) resolveTypesInField(root Builder, seen []Builder, deps *dependencies, flb *FieldBuilder) error {
	if flb.fieldType.foreignMsgType != nil {
		deps.add(flb.fieldType.foreignMsgType.ParentFile())
	} else if flb.fieldType.foreignEnumType != nil {
		deps.add(flb.fieldType.foreignEnumType.ParentFile())
	} else if flb.fieldType.localMsgType != nil {
		if flb.fieldType.localMsgType == flb.msgType {
			return r.resolveTypesInMessage(root, seen, deps, flb.msgType)
		} else {
			return r.resolveType(root, seen, flb.fieldType.localMsgType, deps)
		}
	} else if flb.fieldType.localEnumType != nil {
		return r.resolveType(root, seen, flb.fieldType.localEnumType, deps)
	}
	return nil
}

func (r *dependencyResolver) resolveType(root Builder, seen []Builder, typeBuilder Builder, deps *dependencies) error {
	otherRoot := getRoot(typeBuilder)
	if root == otherRoot {
		// local reference, so it will get resolved when we finish resolving this root
		return nil
	}
	fd, err := r.resolveElement(otherRoot, seen)
	if err != nil {
		return err
	}
	deps.add(fd)
	return nil
}

func (r *dependencyResolver) resolveTypesInFileOptions(root Builder, deps *dependencies, fb *FileBuilder) error {
	for _, mb := range fb.messages {
		if err := r.resolveTypesInMessageOptions(root, deps, mb); err != nil {
			return err
		}
	}
	for _, eb := range fb.enums {
		if err := r.resolveTypesInEnumOptions(root, deps, eb); err != nil {
			return err
		}
	}
	for _, exb := range fb.extensions {
		if err := r.resolveTypesInOptions(root, deps, exb.Options); err != nil {
			return err
		}
	}
	for _, sb := range fb.services {
		for _, mtb := range sb.methods {
			if err := r.resolveTypesInOptions(root, deps, mtb.Options); err != nil {
				return err
			}
		}
		if err := r.resolveTypesInOptions(root, deps, sb.Options); err != nil {
			return err
		}
	}
	return r.resolveTypesInOptions(root, deps, fb.Options)
}

func (r *dependencyResolver) resolveTypesInMessageOptions(root Builder, deps *dependencies, mb *MessageBuilder) error {
	for _, b := range mb.fieldsAndOneOfs {
		if flb, ok := b.(*FieldBuilder); ok {
			if err := r.resolveTypesInOptions(root, deps, flb.Options); err != nil {
				return err
			}
		} else {
			oob := b.(*OneOfBuilder)
			for _, flb := range oob.choices {
				if err := r.resolveTypesInOptions(root, deps, flb.Options); err != nil {
					return err
				}
			}
			if err := r.resolveTypesInOptions(root, deps, oob.Options); err != nil {
				return err
			}
		}
	}
	for _, extr := range mb.ExtensionRanges {
		if err := r.resolveTypesInOptions(root, deps, extr.Options); err != nil {
			return err
		}
	}
	for _, eb := range mb.nestedEnums {
		if err := r.resolveTypesInEnumOptions(root, deps, eb); err != nil {
			return err
		}
	}
	for _, nmb := range mb.nestedMessages {
		if err := r.resolveTypesInMessageOptions(root, deps, nmb); err != nil {
			return err
		}
	}
	for _, exb := range mb.nestedExtensions {
		if err := r.resolveTypesInOptions(root, deps, exb.Options); err != nil {
			return err
		}
	}
	if err := r.resolveTypesInOptions(root, deps, mb.Options); err != nil {
		return err
	}
	return nil
}

func (r *dependencyResolver) resolveTypesInEnumOptions(root Builder, deps *dependencies, eb *EnumBuilder) error {
	for _, evb := range eb.values {
		if err := r.resolveTypesInOptions(root, deps, evb.Options); err != nil {
			return err
		}
	}
	if err := r.resolveTypesInOptions(root, deps, eb.Options); err != nil {
		return err
	}
	return nil
}

func (r *dependencyResolver) resolveTypesInOptions(root Builder, deps *dependencies, opts proto.Message) error {
	// nothing to see if opts is nil
	if opts == nil {
		return nil
	}
	if rv := reflect.ValueOf(opts); rv.Kind() == reflect.Ptr && rv.IsNil() {
		return nil
	}

	msgName := opts.ProtoReflect().Descriptor().FullName()

	opts.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		// make sure we have dependencies on all files that declare custom options
		if fd.IsExtension() {
			deps.add(fd.ParentFile())
		}
		return true
	})

	// we also have to scrutinize the unknown fields on options, for unrecognized options
	

	extdescs, err := proto.ExtensionDescs(opts)
	if err != nil {
		return err
	}
	for _, ed := range extdescs {
		// see if known dependencies have this option
		extd := deps.exts.FindExtension(msgName, ed.Field)
		if extd != nil {
			// yep! nothing else to do
			continue
		}
		// see if this extension is defined in *this* builder
		if findExtension(root, msgName, ed.Field) {
			// yep!
			continue
		}
		// see if configured extension registry knows about it
		extd = r.opts.Extensions.FindExtension(msgName, ed.Field)
		if extd != nil {
			// extension registry recognized it!
			deps.add(extd.GetFile())
		} else if ed.Filename != "" {
			// if filename is filled in, then this extension is a known
			// extension (registered in proto package)
			fd, err := desc.LoadFileDescriptor(ed.Filename)
			if err != nil {
				return err
			}
			deps.add(fd)
		} else if r.opts.RequireInterpretedOptions {
			// we require options to be interpreted but are not able to!
			return fmt.Errorf("could not interpret custom option for %s, tag %d", msgName, ed.Field)
		}
	}
	return nil
}

func findExtension(b Builder, messageName protoreflect.FullName, extTag protoreflect.FieldNumber) bool {
	if fb, ok := b.(*FileBuilder); ok && findExtensionInFile(fb, messageName, extTag) {
		return true
	}
	if mb, ok := b.(*MessageBuilder); ok && findExtensionInMessage(mb, messageName, extTag) {
		return true
	}
	return false
}

func findExtensionInFile(fb *FileBuilder, messageName protoreflect.FullName, extTag protoreflect.FieldNumber) bool {
	for _, extb := range fb.extensions {
		if extb.GetExtendeeTypeName() == messageName && extb.number == extTag {
			return true
		}
	}
	for _, mb := range fb.messages {
		if findExtensionInMessage(mb, messageName, extTag) {
			return true
		}
	}
	return false
}

func findExtensionInMessage(mb *MessageBuilder, messageName protoreflect.FullName, extTag protoreflect.FieldNumber) bool {
	for _, extb := range mb.nestedExtensions {
		if extb.GetExtendeeTypeName() == messageName && extb.number == extTag {
			return true
		}
	}
	for _, mb := range mb.nestedMessages {
		if findExtensionInMessage(mb, messageName, extTag) {
			return true
		}
	}
	return false
}
