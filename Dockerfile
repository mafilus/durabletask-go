# syntax=docker/dockerfile:1

FROM golang:1.26 AS build

COPY . /root
WORKDIR /root

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /durabletask-go

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /durabletask-go /

EXPOSE 4001

# Run
ENTRYPOINT [ "/durabletask-go" ]
CMD [ "--host", "127.0.0.1", "--port", "4001" ]
