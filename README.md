<p align="center">
  <a href="https://hellofresh.com">
    <img width="120" src="https://www.hellofresh.de/images/hellofresh/press/HelloFresh_Logo.png">
  </a>
</p>

# hellofresh/engine

[![Build Status](https://travis-ci.org/hellofresh/goengine.svg?branch=master)](https://travis-ci.org/hellofresh/goengine)

Welcome to HelloFresh GoEngine!!

GoEngine provides you all the capabilities to build an Event sourced application in go.
This was based on the initial project [Engine](https://github.com/hellofresh/engine) for PHP

## Components

Engine is divided in a few small independent components. 

* [CommandBus](commandbus)
* [EventBus](eventbus)
* [EventDispatcher](eventdispatcher)
* [EventSourcing](eventsourcing)
* [EventStore](eventstore)

## Install

```sh
go get -u github.com/hellofresh/goengine
```

## Usage

Here you can check a small tutorial of how to use this component in an orders scenario.

[Tutorial](docs/how_to.md)

## Logging

GoEngine uses default `log` package for debug logging. If you want to use your own logger - `goengine.SetLogHandler()`
is available. Here is how you can use, e.g. `github.com/sirupsen/logrus` for logging:

```go
package main

import (
    "github.com/hellofresh/goengine"
    log "github.com/sirupsen/logrus"
)

func main() {
    goengine.SetLogHandler(func(msg string, fields map[string]interface{}, err error) {
        if nil == fields && nil == err {
            log.Debug(msg)
        } else {
            var entry *log.Entry
            if fields != nil {
                entry = log.WithFields(log.Fields(fields))
                if nil != err {
                    entry = entry.WithError(err)
                }
            } else {
                entry = log.WithError(err)
            }

            entry.Debug(msg)
        }
    })

    // do your application stuff
}
```

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.

