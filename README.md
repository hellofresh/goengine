# GoEngine

GoEngine is an Event Sourcing library written for GoLang.

The goal of this library is to reduce the amount of time you have to spend thinking about the infrastructure so you can focus on  
implementing your Domains and Business logic!

## Installation

```BASH
go get -u github.com/hellofresh/goengine
```

## Documentation

Check out our [quick start guide][goengine-book-quick-start] which is part of your [GoEngine docs][goengine-book].
If you prefer to be closer to the code you can always refer to [GoDoc][godoc].

## RoadMap

The following features are planned for the future (in no specific order)

* Improve documentation and examples
* Support for Snapshots
* Inmemory Projection support
* Creating Linked EventStreams
* Distributes tracing (using [opencensus](https://opencensus.io/) and/or [opentracing](https://opentracing.io/))
* ...

## Contributing 

We encourage and support an active, healthy community of contributors — including you! 
Details are in the [contribution guide](CONTRIBUTING.md) and the [code of conduct](CODE_OF_CONDUCT.md). 

[godoc]: https://godoc.org/github.com/hellofresh/goengine
[goengine-book]: https://goengine.readthedocs.io/en/latest/
[goengine-book-quick-start]: https://goengine.readthedocs.io/en/latest/quick-start/
