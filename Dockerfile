# syntax=docker/dockerfile:1.6
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY . ./
RUN CGO_ENABLED=0 GOCACHE=/tmp/.gocache go build -o /bin/kv-vs ./cmd/api

FROM gcr.io/distroless/base-debian12
COPY --from=build /bin/kv-vs /bin/kv-vs
EXPOSE 8080
ENTRYPOINT ["/bin/kv-vs"]
