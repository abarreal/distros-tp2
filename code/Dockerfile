FROM golang:1.16

RUN mkdir -p /build
WORKDIR /build/
COPY . .
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/node tp2.aba.distros.fi.uba.ar

FROM busybox:latest
COPY --from=0 /build/bin/node /node
ENTRYPOINT ["/bin/sh"]