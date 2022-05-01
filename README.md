# EventBus 3
A reliable event/message hub for boosting Event-Driven architecture &amp; big data ingestion.

TODO:
x optimize configs structure
x optimize producer
x test http_app
x test producer
x test topic_resolver
x test signals on producer and topic_resolver 
x remove old configs
- try k8s, confirm the deployment and config change strategies
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
- test consumer abnormal cases
x test consumer for new created topics
x try config map updates
x fix the config change function
  d split config files
  x make sure if that the config is invalid does not affect running status    
- k8s scripts
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
- 
- check filemtime on kubenetes [y]
- refactor to use filemtime [y]
- test changing the delay rebalance parameter on broker config
- improve config structure
- refactor commit offset to use store

FOLLOWING PLAN:
- lab env
- staging and prod env
- new kafka cluster
- consume from backup cluster
- CI/CD
- monitoring