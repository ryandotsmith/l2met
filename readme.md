# l2met

Convert your log stream into charts and actionable alerts in less than 1 minute
with 0 software installation.

## Setup

Visit your [Librato account page](https://metrics.librato.com/account).

![img](http://f.cl.ly/items/3f3S382I352E2Q2C0Q44/Screen%20Shot%202012-10-22%20at%209.14.41%20PM.png)

Copy your email & token.

Visit your [l2met account page](https://www.l2met.net/).

![img](http://f.cl.ly/items/230p0B0b0h2u2A341c24/Screen%20Shot%202012-10-22%20at%209.18.56%20PM.png)

Click **+drain** and paste your credentials into the form. Submit the form.

Copy the URL.

Add the URL as a drain onto your Heroku app.

```bash
$ heroku sudo passes:add logplex-beta-program
$ heroku drains:add https://drain.l2met.net/consumers/your-token/logs
```

## Log Conventions

### Counter

Metrics Produced:

* app.module.function.count

```
measure="app.module.function"
```

### Sample

Samples are useful for building metrics around time based functions. For instance, the elapsed duration of a function call. Or you can measure the value of an in memory resource.

Metrics Produced:

* app.module.function.min
* app.module.function.max
* app.module.function.mean
* app.module.function.median
* app.module.function.perc95
* app.module.function.perc99
* app.module.function.last

Protocol:

```
measure="app.module.function" val=42
```

Examples:

```
measure="core.apps.get" val=1.23 units=s
```

In the previous example we have an app named **core** which has an HTTP GET endpoint named **apps** that took 1.23 seconds to execute. The *units* key/value is optional. Providing *units* will allow grouping by units on the chart UI.


```
measure="nile.r53-backlog" val=42 units=items
```

This example will provide us with metrics around the backlog of our Route53 queue.
## Arch

High level:

```
heroku app -> http log drains -> l2met -> librato
```

Inside of l2met:

```
l2met/web -> l2met/receiver -> l2met/register -> redis <- l2met/db-outlet -> librato/metrics
```
