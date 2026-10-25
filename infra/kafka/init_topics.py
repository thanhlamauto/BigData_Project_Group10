"""Idempotently create the raw/cleaned Kafka topics using the same client as ingestion."""

import argparse
import os


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--raw-topic", default=os.getenv("RAW_TOPIC", "streamrag.raw.v1"))
    parser.add_argument(
        "--cleaned-topic", default=os.getenv("CLEANED_TOPIC", "streamrag.cleaned.v1")
    )
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args()
    if args.partitions < 1 or args.replication_factor < 1:
        parser.error("partitions and replication-factor must be >= 1")
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    topics = [
        NewTopic(name, num_partitions=args.partitions, replication_factor=args.replication_factor)
        for name in dict.fromkeys((args.raw_topic, args.cleaned_topic))
    ]
    futures = admin.create_topics(topics, request_timeout=30)
    failed = False
    for name, future in futures.items():
        try:
            future.result()
            print(f"created topic={name} partitions={args.partitions}")
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"exists topic={name} (configuration unchanged)")
            else:
                failed = True
                print(f"failed topic={name}: {exc}")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
