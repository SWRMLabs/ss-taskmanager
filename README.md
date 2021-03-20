# ss-taskmanager

Async task manager. Tasks can easily be customized and executed asynchronously on
the next available worker.

The manager keeps workers ready to multiplex tasks. The maximum no. of workers can
be configured.

This package was mainly created to abstract all async functionality from the app. It
provides a consistent context interface to manage routine lifecycle from a single
place.

## Install
`ss-taskmanager` works like a regular Go module:

```
> go get github.com/SWRMLabs/ss-taskmanager
```

## Usage
```
import "github.com/SWRMLabs/ss-taskmanager"
```
Check tests for examples
