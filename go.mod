module github.com/jhump/protoreflect

go 1.13

require (
	github.com/golang/protobuf v1.4.1
	github.com/gordonklaus/ineffassign v0.0.0-20200309095847-7953dde2c7bf // indirect
	github.com/nishanths/predeclared v0.0.0-20200524104333-86fad755b4d3 // indirect
	golang.org/x/net v0.0.0-20200226121028-0de0cce0169b
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.24.0
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
)

replace google.golang.org/protobuf => ../../../google.golang.org/protobuf
