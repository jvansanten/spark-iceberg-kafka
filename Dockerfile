FROM maven:3.10.0-eclipse-temurin-17@sha256:b67db904974d04b9e6eba5b073ae227569ee1cae0d1f26be8993525291ef76dc AS dependencies

WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -ntp package -DskipTests \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/build/dependencies

FROM docker.io/apache/spark:4.1.3-python3

COPY --from=dependencies /build/dependencies/ /opt/spark/jars/
COPY --from=dependencies /build/target/spark-iceberg-kafka-image-1.0.0.jar /opt/spark/app/alerts-to-kafka.jar

RUN /opt/spark/bin/spark-submit --version