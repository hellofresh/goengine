# GoEngine [![GitHub][license-img]][license] [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Code Coverage][cov-img]][cov] [![Go Report Card][go-report-img]][go-report]

GoEngine is an Event Sourcing library written for GoLang.

The goal of this library is to reduce the amount of time you have to spend thinking about the infrastructure so you can focus on  
implementing your Domains and Business logic!

## Fork

This project was forked to experiment with some assumptions (e.g. can we have an aggregate ID that is not a UUID?) and to improve few things like PSQL code generation.
These changes will also be raised as PRs to the main repo.

## Installation

```BASH
go get -u github.com/vimeda/goengine
```

## Documentation

Check out our [quick start guide][goengine-book-quick-start] which is part of your [GoEngine docs][goengine-book].
If you prefer to be closer to the code you can always refer to [GoDoc][doc].

## RoadMap

The following features are planned for the future (in no specific order)

* Improve documentation and examples
* Support for Snapshots
* Inmemory Projection support
* Creating Linked EventStreams
* Distributes tracing (using [opencensus](https://opencensus.io/) and/or [opentracing](https://opentracing.io/))
* ...

## Contributing

Contributions, issues and feature requests are welcome!<br />Feel free to check [issues page](https://github.com/vimeda/goengine/issues).
