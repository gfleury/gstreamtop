FROM golang:alpine
RUN mkdir -p /go/src/github.com/gfleury/gstreamtop
ADD . /go/src/github.com/gfleury/gstreamtop/
WORKDIR /go/src/github.com/gfleury/gstreamtop/
RUN rm -rf /go/src/github.com/gfleury/gstreamtop/examples/
RUN rm -rf /go/src/github.com/gfleury/gstreamtop/dist/
RUN go build
CMD ["./gstreamtop"]
