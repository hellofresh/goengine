FROM golang:1.7

ENV APP_DIR $GOPATH/src/github.com/hellofresh/goengine

RUN mkdir -p $APP_DIR

COPY . $APP_DIR
WORKDIR $APP_DIR

RUN make

CMD ["go-wrapper", "run"]