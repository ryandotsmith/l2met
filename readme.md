## l2met

### Using L2met

```bash
$ curl -X POST https://l2met.herokuapp.com/consumers \
  -d "email=ryan@heroku.com" \
  -d "token=my-librato-token"
l2met-secure-id

$ heroku drains:add https://l2met.herokuapp.com/consumers/l2met-secure-id/logs
```

Convert structured log streams into Librato metrics.

```
2012-08-08T06:14:03+00:00 app[web.16]: app=shushu ns=utils measure=true fn=resources-hid-billable-events-eid elapsed=0.295
```

![img](http://f.cl.ly/items/0U1T1G082m3T0W2U4337/Screen%20Shot%202012-08-08%20at%209.06.42%20AM.png)

### Deploy to Heroku

Be sure and set config variables defined in lib/l2met/config.rb.

```bash
$ git clone git://github.com/ryandotsmith/l2met.git
$ cd l2met
$ heroku create
$ git add . ; git commit -am "init"
$ git push heroku master
$ heroku scale web=2 dboutlet=2
```
