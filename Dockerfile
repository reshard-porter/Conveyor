# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/conveyor ./cmd/conveyor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/conveyor /conveyor
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/conveyor"]
CMD ["serve"]
