# kafka-util

Kafka utilities meant to supplement existing tools such as `kafka-topics.sh`, `kafka-reassign-partitions.sh`, and `kafka-leader-election.sh`.

## Kafka bootstrap server

A Kafka bootstrap server needs to be passed in either with the `--bootstrap-server` flag or the `KAFKA_BOOTSTRAP_SERVER` environment variable.

## Ongoing reassignments

To list ongoing reassignments:
```shell
kafka-util ongoing
```

To cancel any ongoing reassignments:
```shell
kafka-util ongoing --cancel
```

To wait for any ongoing reassignments to complete:
```shell
kafka-util ongoing --wait
```

## Reassign a single partition

To set the replica set of partition `0` of topic `foobar` to `{0, 1, 2}`:
```shell
kafka-util reassign -t foobar -p 0 -r 0,1,2
```

Do the above, but wait for the reassignment to complete before returning:
```shell
kafka-util reassign -t foobar -p 0 -r 0,1,2 && kafka-util ongoing --wait
```

## Reassign multiple partitions with a per-broker concurrency limit

The `stage` subcommand executes a series of partition reassignments.

The partition reassignments are given as a JSON file in the same format that the
'kafka-reassign-partitions.sh' utility generates:

```json
{
  "partitions": [
    {
      "topic": "foo",
      "partition": 1,
      "replicas": [1,2,3],
      "log_dirs": ["dir1", "dir2", "dir3"]
    }
  ],
  "version": 1
}
```

The `--max-moves-per-broker` flag specifies the maximum number of simultaneous
inter-broker replica movements allowed per broker. A broker is considered part
of a replica movement if it is either the source or destination broker of the
replica or is the lead replica of the partition.

To execute a manual assignment json file with a concurrency limit of 1:
```shell
kafka-util stage --reassignment-json-file reassignments.json --max-moves-per-broker 1
```

Use the `--dry-run` flag to print the reassignments without actually executing them:
```shell
kafka-util stage --reassignment-json-file reassignments.json --max-moves-per-broker 1 --dry-run
```
