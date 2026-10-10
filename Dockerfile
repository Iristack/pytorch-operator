FROM golang:1.13 AS build-image

ADD . /go/src/github.com/kubeflow/pytorch-operator

WORKDIR /go/src/github.com/kubeflow/pytorch-operator

# Build pytorch operator v1 binary
RUN CGO_ENABLED=0 go build -mod=vendor ./cmd/pytorch-operator.v1

#FROM registry.access.redhat.com/ubi8/ubi:latest
FROM registry-cn-hangzhou.ack.aliyuncs.com/dev/debian:11-base

COPY --from=build-image /go/src/github.com/kubeflow/pytorch-operator/pytorch-operator.v1 /pytorch-operator.v1

COPY third_party/library/license.txt /license.txt

ENTRYPOINT ["/pytorch-operator.v1", "-alsologtostderr"]
