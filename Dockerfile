FROM golang:alpine
RUN mkdir -p /go/src/github.com/gfleury/gstreamtop
ADD . /go/src/github.com/gfleury/gstreamtop/
WORKDIR /go/src/github.com/gfleury/gstreamtop/
RUN go build
CMD ["./gstreamtop"]