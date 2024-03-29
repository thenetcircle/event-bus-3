# EventBus v3

[![pipeline status](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/badges/main/pipeline.svg)](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/-/commits/main) 
[![test coverage](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/badges/main/coverage.svg?job=coverage)](http://gitlab.thenetcircle.lab/tnc-service-team/eventbus3/badges/main/coverage.svg?job=coverage)

## Deploying

### Lab

Pushing changes to gitlab will run the tests and build the image, then deploy 
the docker image to `fat`.

### Staging

Running on `cloud-host-xx`. Deployment command:

```shell
docker pull cloud-host-01.austria.private:5001/library/my-service:latest
docker run -d -p 7760:8080 \
    -v /data/some-folder:/code/app/data \
    --name my-service-staging \
    --log-opt max-size=100m \
    --log-opt max-file=1 \
    cloud-host-01.austria.private:5001/library/my-service:latest
```

### Production

Running on `cloud-host-xx`. Deployment command:

```shell
docker pull cloud-host-01.austria.private:5001/library/my-service:latest
docker run -d -p 7770:8080 \
    -v /data/some-folder:/code/app/data \
    --name my-service-prod \
    --log-opt max-size=100m \
    --log-opt max-file=1 \
    --name my-service \
    cloud-host-01.austria.private:5001/library/my-service:latest
```

### Starting/stopping

Starting it if it's down:

```shell
docker restart quizmatch-service-fetiprod
```

### Logs

Use `--since` to get less logs. Pipe to `stdout` and paginate using `less`:

```shell
docker logs --since '2024-01-18T00:00:00' my-service-prod 2>&1 | less
```

## Features & Change Logs
A reliable event/message hub for boosting Event-Driven architecture &amp; big data ingestion.

TODO:
x optimize configs structure
x optimize producer
x test http_app
x test producer
x test topic_resolver
x test signals on producer and topic_resolver 
x remove old configs
x try k8s, confirm the deployment and config change strategies
x try app_consumer update config
x add producer into consumer 
x complete app_consumer
x refactor thread exit mechanism
x check producer error handler
x producer update config 
x consumer config changes, and config producer config changes
x if consumer downstream blocks, will the upstream be waiting? 
x producer log with caller id
- Sentry
x test consumer abnormal cases
x test consumer for new created topics
x try config map updates
x fix the config change function
  d split config files
  x make sure if that the config is invalid does not affect running status    
x k8s scripts
x after update producer config -- can not reproduce
```shell
Exception in callback <built-in method set_exception of _asyncio.Future object at 0x1067917c0>
handle: <Handle Future.set_exception>
Traceback (most recent call last):
  File "uvloop/cbhandles.pyx", line 63, in uvloop.loop.Handle._run
asyncio.exceptions.InvalidStateError: invalid state
Exception in callback <built-in method set_exception of _asyncio.Future object at 0x1067915c0>
handle: <Handle Future.set_exception>
Traceback (most recent call last):
  File "uvloop/cbhandles.pyx", line 63, in uvloop.loop.Handle._run
asyncio.exceptions.InvalidStateError: invalid state
Exception in callback <built-in method set_exception of _asyncio.Future object at 0x10679f840>
handle: <Handle Future.set_exception>
Traceback (most recent call last):
  File "uvloop/cbhandles.pyx", line 63, in uvloop.loop.Handle._run
asyncio.exceptions.InvalidStateError: invalid state
[2022-04-03 22:54:21 +0800] [74503] [INFO] Handling signal: winch
```
x check filemtime on kubenetes 
x refactor to use filemtime
x test changing the delay rebalance parameter on broker config
x improve config structure
x add consumer status (enable or disable)
x refactor commit offset to use store
x decrease the queues size
x test commit message instead of offsets
- continue to do the stress test
- change subscribed topics won't make a rebalance
x update producer borker the main_producer didn't restart
- log consumer start config
- test the consumer change topic but assigned old topic issue
- add a high offset protection code
- move new kafka from ch14,15,16 to gpu04,05,06
- test same consumer group subscribe to different topics

New:
- remove that too much info sending to Sentry
- when retry failed msg, don't send everything to Sentry

FOLLOWING PLAN:
x lab env
x staging and prod env
x new kafka cluster
- consume from backup cluster
- CI/CD
- monitoring
