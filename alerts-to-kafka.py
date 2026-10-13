import json
import time
from argparse import ArgumentParser, FileType
from urllib.request import urlopen

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.classic.column import Column, _to_java_column


def to_avro_abris_config(config_map, schema_id):
    """
    Create to avro abris config with a schema url

    :param config_map (dict[str, str]): configuration map to pass to the serializer, ex: {'schema.registry.url': 'http://localhost:8081'}
    :param schema_id (int): the ID of the schema to use
    :return: za.co.absa.abris.config.ToAvroConfig
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return (
        jvm_gateway.za.co.absa.abris.config.AbrisConfig.toConfluentAvro()
        .downloadSchemaById(schema_id)
        .usingSchemaRegistry(scala_map)
    )


def to_avro(col, config):
    """
    avro serialize
    :param col (PySpark column / str): column name "key" or "value"
    :param config (za.co.absa.abris.config.ToAvroConfig): abris config, generated from abris_config helper function
    :return: PySpark Column
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.to_avro(_to_java_column(col), config)).alias(col)


parser = ArgumentParser(
    description="Query alerts from Iceberg and write them to a Kafka topic in Avro format"
)
parser.add_argument(
    "--where",
    default="""
        diaSource.diaObjectId > 0
        and diaSource.reliability > 0.95
        and diaSource.visit >= 2026040100000
        and diaSource.visit < 2026043000000
        and diaSource.psfFlux > 0
    """,
    help="SQL WHERE clause to filter alerts",
)
parser.add_argument(
    "--limit",
    default=None,
    type=int,
    help="Limit the number of alerts to send. Note that setting a limit does not actually make the query any faster, and delays message delivery until the scan is finished.",
)
parser.add_argument(
    "--order-by",
    default=None,
    help="SQL ORDER BY clause to sort alerts. Note that like limit, this delays message delivery until the scan is finished, and does not make the query any faster.",
)
parser.add_argument(
    "--schema-registry",
    default="https://rubin-alert-schemas.slac.stanford.edu/schema-registry",
    help="URL of the schema registry to fetch Avro schemas from",
)
parser.add_argument(
    "--subject",
    default="alert-packet",
    help="Schema subject name in the schema registry. The latest schema for this subject will be used for Avro serialization",
)
parser.add_argument(
    "--schema-exclude",
    nargs="+",
    default={
        "ssSource",
        "mpc_orbits",
        "cutoutTemplate",
        "cutoutScience",
        "cutoutDifference",
    },
    type=set,
    help="List of schema fields to exclude from the query",
)
parser.add_argument(
    "--topic", default="lsst-ampel-dev-alerts", help="Kafka topic to write alerts to"
)
parser.add_argument(
    "--kafka-bootstrap-servers",
    default="kafka-kafka-bootstrap.lsst-kafka.svc.cluster.local:9092",
    help="Comma-separated list of Kafka bootstrap servers (default: kafka-kafka-bootstrap.lsst-kafka.svc.cluster.local:9092)",
)
parser.add_argument(
    "--kafka-sasl-jaas-config",
    default="/kafka-user/sasl.jaas.config",
    type=FileType("r"),
    help="Path to the Kafka SASL JAAS config file",
)
parser.add_argument(
    "--kafka-security-protocol",
    default="SASL_PLAINTEXT",
    help="Kafka security protocol",
)
parser.add_argument(
    "--kafka-security-mechanism",
    default="SCRAM-SHA-512",
    help="Kafka security mechanism",
)
parser.add_argument(
    "--kafka-timeout-ms", default="3000", help="Kafka producer timeout in milliseconds"
)
parser.add_argument(
    "--wait",
    default=0,
    type=int,
    help="Wait time in seconds before exiting. This is useful for keeping the Spark driver alive for inspection after the job is done.",
)

args = parser.parse_args()

# hacky way to get the latest schema
with urlopen(f"{args.schema_registry}/subjects/{args.subject}/versions") as response:
    schema_id = max(json.load(response))
with urlopen(
    f"{args.schema_registry}/subjects/{args.subject}/versions/{schema_id}"
) as response:
    schema = json.loads(json.load(response)["schema"])
columns = [f["name"] for f in schema["fields"] if f["name"] not in args.schema_exclude]

query = f"""select
    struct(
    {", ".join(columns)}
    ) as value
    from lsst.alerts where {args.where}"""
if args.order_by:
    query += f" order by {args.order_by}"
if args.limit:
    query += f" limit {args.limit}"

# Create a SparkSession
spark = SparkSession.builder.appName("AlertsToKafka").getOrCreate()

abris_config = to_avro_abris_config(
    config_map={"schema.registry.url": args.schema_registry}, schema_id=schema_id
)

try:
    df = spark.sql(query).select(to_avro("value", abris_config))

    (
        df.write.format("kafka")
        .options(
            **{
                "kafka.bootstrap.servers": args.kafka_bootstrap_servers,
                "kafka.security.protocol": args.kafka_security_protocol,
                "kafka.sasl.mechanism": args.kafka_security_mechanism,
                "kafka.sasl.jaas.config": args.kafka_sasl_jaas_config.read(),
                "kafka.timeout.ms": args.kafka_timeout_ms,
                "topic": args.topic,
            }
        )
        .save()
    )
finally:
    if args.wait > 0:
        print(
            f"Write finished; keeping driver alive for {args.wait} seconds for inspection"
        )
        time.sleep(args.wait)
    spark.stop()
