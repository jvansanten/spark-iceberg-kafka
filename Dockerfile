FROM maven:3.9.9-eclipse-temurin-17 AS dependencies

WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -ntp package \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/build/dependencies

FROM docker.io/apache/spark:4.1.3-python3

COPY --from=dependencies /build/dependencies/ /opt/spark/jars/
COPY --from=dependencies /build/target/spark-iceberg-kafka-image-1.0.0.jar /opt/spark/app/alerts-to-kafka.jar

RUN test -f /opt/spark/app/alerts-to-kafka.jar \
    && test -f /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
    && test -f /opt/spark/jars/iceberg-aws-bundle-1.12.0.jar \
    && test -f /opt/spark/jars/abris_2.13-7.0.0-RC1.jar \
    && test -f /opt/spark/jars/kafka-clients-4.3.1.jar \
    && test -f /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.2.0.jar \
    && /opt/spark/bin/spark-submit --version