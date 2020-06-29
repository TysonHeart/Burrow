FROM golang:1.11-alpine as builder

ENV DEP_VERSION="0.5.0"
RUN apk add --no-cache git curl gcc libc-dev && \
	curl -L -s https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 -o $GOPATH/bin/dep && \
	chmod +x $GOPATH/bin/dep && \
	mkdir -p $GOPATH/src/github.com/tysonheart/burrow

ADD . $GOPATH/src/github.com/tysonheart/burrow/
RUN cd $GOPATH/src/github.com/tysonheart/burrow && \
	dep ensure && \
	go build -o /tmp/burrow .

FROM iron/go
LABEL maintainer="LinkedIn Burrow https://github.com/tysonheart/burrow"

WORKDIR /app
COPY --from=builder /tmp/burrow /app/
ADD /docker-config/burrow.toml /etc/burrow/

CMD ["/app/burrow", "--config-dir", "/etc/burrow"]
