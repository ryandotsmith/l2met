# l2met

Convert your log stream into charts and actionable alerts in less than 1 minute
with 0 software installation.

## Log Conventions

### Counter Metric

Metrics Produced:

* app.module.function.count

```
measure="app.module.function"
```

### Sample Metric

Samples are useful for building metrics around time based functions. For instance, the elapsed duration of a function call. Or you can measure the value of an in memory resource.

Metrics Produced:

* app.module.function.min
* app.module.function.max
* app.module.function.mean
* app.module.function.median
* app.module.function.perc95
* app.module.function.perc99
* app.module.function.last
* app.module.function.count

Protocol:

```
measure="app.module.function" val=42
```

Examples:

```
measure="core.apps.get" val=1.23
```

In the previous example we have an app named **core** which has an HTTP GET endpoint named **apps** that took 1.23 seconds to execute.
