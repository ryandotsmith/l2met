# Beta Features

* Log Convetnions
* Last Value Computation


## Log Conventions

### Counter

Metrics Produced:

* app.module.function.count

```
app="app" measure="module.function"
```

### Value Counter

Value coutners are useful for building metrics around time based functions. For instance, the elapsed duration of a function call. Or you can measure the value of an in memory resource.

Metrics Produced:

* app.module.function.min
* app.module.function.max
* app.module.function.mean
* app.module.function.median
* app.module.function.perc95
* app.module.function.perc99

Protocol:

```
app="app" measure="module.function" val=42
```

Examples:

```
app="core" measure="apps.get" val=1.23 units=s
```

In the previous example we have an app named **core** which has an HTTP GET endpoint named **apps** that took 1.23 seconds to execute. The *units* key/value is optional. Providing *units* will allow grouping by units on the chart UI.


```
app="nile" measure="r53-backlog" val=42 units=items
```

This example will provide us with metrics around the backlog of our Route53 queue.
