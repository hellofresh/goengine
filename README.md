<p align="center">
  <a href="https://hellofresh.com">
    <img width="120" src="https://www.hellofresh.de/images/hellofresh/press/HelloFresh_Logo.png">
  </a>
</p>

# hellofresh/engine

[![Build Status](https://travis-ci.org/hellofresh/goengine.svg?branch=master)](https://travis-ci.org/hellofresh/goengine)

Welcome to HelloFresh GoEngine!!

GoEngine provides you all the capabilities to build an Event sourced application in go
This was based on the initial project [Engine](https://github.com/hellofresh/engine) for PHP

## Components

Engine is divided in a few small independent components. 

* [CommandBus](src/commandbus/README.md)
* [EventBus](src/eventbus/README.md)
* [EventDispatcher](src/eventdispatcher/README.md)
* [EventSourcing](src/eventsourcing/README.md)
* [EventStore](src/eventstore/README.md)

## Install

```sh
go get github.com/hellofresh/goengine
```

## Usage

Here you can check a small tutorial of how to use this component in an orders scenario.

[Tutorial] (docs/how_to.md)

## Contributing

Please see [CONTRIBUTING](CONTRIBUTING.md) for details.

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.

