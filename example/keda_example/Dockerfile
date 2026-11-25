# Build from the repository root so that the module replace directive can
# resolve the library from the working tree:
#
#   docker build -f example/keda_example/Dockerfile -t agilepool-keda-example:1 .
#
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
WORKDIR /src/example/keda_example
RUN CGO_ENABLED=0 go build -o /out/keda-example .

FROM alpine:3.20
COPY --from=build /out/keda-example /keda-example
EXPOSE 8080
ENTRYPOINT ["/keda-example"]
