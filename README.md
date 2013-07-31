# l2met

Turn these:
```ruby
$stdout.puts("measure.db.latency=4ms")
```

Into this:

![img](http://f.cl.ly/items/2R0h1x1b3V0Y0z0l1t1n/Screen%20Shot%202013-07-30%20at%209.59.52%20PM.png)


L2met receives HTTP requests that contain a body of RFC5424 formatted data. Commonly, data is drained into l2met by [logplex](https://github.com/heroku/logplex) or [log-shuttle](https://github.com/heroku/log-shuttle). Once data is delivered, l2met extracts and parses the log lines using the [logging conventions](https://github.com/ryandotsmith/l2met/wiki/Usage#log-conventions) and then stores the data in redis so that l2met outlets can read the data, build metrics, and deliver the metrics to your Librato account.

Checkout the wiki for information related to: [usage](https://github.com/ryandotsmith/l2met/wiki/Usage), [archetecture](https://github.com/ryandotsmith/l2met/wiki/Architecture), and [administration](https://github.com/ryandotsmith/l2met/wiki/Administration).

## Getting Started

The easiest way to get l2met up and running is to deploy to Heroku. This guide assumes you have already created a Heroku & Librato account.

```bash
$ curl https://s3-us-west-2.amazonaws.com/l2met/v2.0beta/linux/amd64/l2met.tar.gz | tar xvz

$ ./setup my-l2met e@foo.com abc123
...
Drain URL: https://long-token@my-l2met.herokuapp.com/logs
```

This command will create Heroku app named *my-l2met* and return a drain URL with encrypted Librato credentials for a Librato account with email *e@foo.com* and an API token of *abc123*. After you have created *my-l2met*, you can add the drain URL to a Heroku app. A copy of the log stream will be delivered to *my-l2met* and metrics will be sent to the Librato account which your provided in the setup.

```bash
$ heroku drains:add https://long-token@my-l2met.herokuapp.com/logs -a myapp
```

You can manually send data to *my-l2met* by the following curl command:

```bash
$ curl "https://long-token@my-l2met.herokuapp.com/logs" --data "94 <190>1 2013-03-27T20:02:24+00:00 hostname token shuttle - - measure.hello=99 measure.world=100"
```

Verify the command worked by viewing the [newly created metrics](https://metrics.librato.com/metrics?search=hello).

## Hacking on l2met
Before working on a new feature, send your proposal to the [mailing list](https://groups.google.com/d/forum/l2met) for tips & feedback. Be sure to work on a feature branch and submit a PR when ready.


### Documentation
[GoDoc](http://godoc.org/github.com/ryandotsmith/l2met)

### Running Tests
[![Build Status](https://drone.io/github.com/ryandotsmith/l2met/status.png)](https://drone.io/github.com/ryandotsmith/l2met/latest)
```bash
$ go version
go version go1.1.1 darwin/amd64
```

```bash
$ git clone git://github.com/ryandotsmith/l2met.git
$ cd l2met
$ export SECRETS=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | openssl base64)
$ export TZ=UTC
$ go test ./...
```
