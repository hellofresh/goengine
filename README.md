# GoEngine [![GitHub][license-img]][license] [![GoDoc][doc-img]][doc] [![Go Report Card][go-report-img]][go-report]

GoEngine is an Event Sourcing library written for GoLang.

The goal of this library is to reduce the amount of time you have to spend thinking about the infrastructure so you can focus on  
implementing your Domains and Business logic!

## Installation

```BASH
go get -u github.com/hellofresh/goengine/v2
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
* Distributes tracing (using [OpenTelemetry](https://opentelemetry.io/)
* ...

## Contributing 

We encourage and support an active, healthy community of contributors — including you! 
Details are in the [contribution guide](CONTRIBUTING.md) and the [code of conduct](CODE_OF_CONDUCT.md). 

------------------
<p align="center">
    <a href="https://hellofresh.com" style="text-decoration:none; margin-right:2rem;">
    <img height="110" src="https://www.hellofresh.de/images/hellofresh/press/HelloFresh_Logo.png">
  </a>
</p>


[doc-img]: https://godoc.org/github.com/hellofresh?status.svg
[doc]: https://godoc.org/github.com/hellofresh/goengine/v2
[cov-img]: https://img.shields.io/codecov/c/github/hellofresh/goengine.svg
[license-img]: https://img.shields.io/github/license/hellofresh/goengine.svg?style=flat
[license]: LICENSE
[go-report-img]:https://goreportcard.com/badge/github.com/hellofresh/goengine
[go-report]: https://goreportcard.com/report/github.com/hellofresh/goengine
[goengine-book]: https://goengine.readthedocs.io/en/latest/
[goengine-book-quick-start]: https://goengine.readthedocs.io/en/latest/quick-start/
