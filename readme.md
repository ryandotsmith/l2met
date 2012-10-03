# l2met

Convert your log stream into charts and actionable alerts in less than 1 minute
with 0 software installation.

## Usage

### Log Data Conventions

L2met uses heuristics to create metrics from log data. Ensure that you have the following style of logs:

#### Time Based Metrics

Including the keys (measure, app, fn, elapsed) will produce:

* counter
* min/max
* mean/median
* perc95/perc99

```
measure=true app=myapp fn="your-fn-name" elapsed=1.23
```

#### Counters

Including the keys (measure, app, at) will count the number of occurences. For example:

```
measure=true app=myapp at="error"
```

#### Last Value

Including the keys (measure, app, at, last) will track the last value of a metric. For instance, tracking the length of a collection over time.

```
measure=true app=myapp at="queue-backlog" last=99
```

## Arch

High level:

```
heroku app -> http log drains -> l2met -> librato
```

Inside of l2met:

```
l2met/web -> l2met/receiver -> l2met/register -> aws/dynamodb <- l2met/db-outlet -> librato/metrics
```

## Credits

Previos attempts at solving the problem:

* [pulse](https://github.com/heroku/pulse)
* [wcld](https://github.com/ryandotsmith/wcld)
* [exprd](https://github.com/heroku/exprd)

l2met is an ongoing quest for platform visibility inspired by:

* [mmcgrana](https://github.com/mmcgrana)
* [mfine](https://github.com/mfine)
