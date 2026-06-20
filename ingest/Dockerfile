FROM --platform=$BUILDPLATFORM golang:1.25 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/ingest ./cmd/ingest

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/ingest /ingest
ENTRYPOINT ["/ingest"]
