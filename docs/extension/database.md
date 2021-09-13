# PQ

In order to work with the Postgres aggregate and stream projector a postgres Listener is provided for the [pq] postgres driver.

```golang
import "github.com/hellofresh/goengine/v2/extension/pq"

listener, err := pq.NewListener(
	s.PostgresDSN,
	string(projection.FromStream()),
	time.Millisecond,
	time.Second,
	s.GetLogger(),
)
```

[pq]: https://github.com/lib/pq
