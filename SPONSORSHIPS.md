# Sponsoring Cete

If you're interested in sponsoring Cete, here's a price list of features
I can prioritise implementing.

As a sponsor, I will also display a medium sized graphic and link to your
company/service near the top of the README for at least a year.

Note that these prices are subject to change in the future.

## Updating dependencies

I will update the dependencies of Cete (Badger and MsgPack), this may take
quite a while as Badger has had some significant changes.

```
Estimated Burden: 5 hours
Cost: $250 AUD
```

## Enhanced Serialization

I will work on improving the serialization/deserialization of Cete which would
improve performance in most workloads. Currently the plan for this would
be to adopt an alternative MsgPack encoder/decoder such as https://github.com/shamaton/msgpack
which has significantly better struct encode/decode performance. Support for
https://github.com/tinylib/msgp would also be added as a code generated
solution. Using tinylib/msgp is expected to speed up encoding/decoding by 10x.

To develop all of this, I would need to create custom forks of 3 MsgPack implementations
(as vmihailenco's will still be kept for its fast Query functionality). This
would take a considerable amount of time to integrate.

```
Estimated Burden: 10 hours
Cost: $500 AUD
```

## Transactions

I will implement transactions as supported by the latest version of Badger to
virutally all the methods of Cete. This would also require a bunch of new tests
which can take a substantial amount of time.
The pre-requisite for this would be to update all the dependencies first.

```
Estimated Burden: 20 hours
Cost: $1000 AUD
```

## Faster Range Pipelines

Currently the way Range pipelines work is suboptimal due to concurrency overhead.
I will attempt to fix this, which should have some performance gains on Range
pipelining/processing. Note that Range queries are already quite fast, so
possibly reconsider or contact me if this is necessary for you.

```
Estimated Burden: 10 hours
Cost: $500 AUD
```

## Custom Optimisations

Want optimisations custom to your workload? I'll be happy to quote you a price
for them, just let me know by emailling [me@chuie.io](mailto:me@chuie.io).


