## Plan: Spark Image in GitLab CI

Build a jar-only image from `docker.io/apache/spark:4.1.3-python3`. Maven will resolve the requested dependencies and transitives from Maven Central and Confluent; the image will place them in Spark’s standard `$SPARK_HOME/jars` directory. Kaniko will publish the image to GitLab Container Registry.

**Steps**
1. Add a root `pom.xml` declaring the five exact dependency coordinates, both Maven repositories, and a dependency-copy step with a predictable output directory. Avoid copying Spark-provided dependencies where Maven marks them as `provided`.
2. Add a multi-stage `Dockerfile`: resolve dependencies in a Maven stage, then copy the jars into `$SPARK_HOME/jars` in the Spark 4.1.3 Python image. Include build-time checks for the requested jars and `spark-submit --version`.
3. Add `.gitlab-ci.yml` for Kaniko. Authenticate using GitLab’s registry variables, publish a commit-SHA tag, and publish `latest` on the default branch.
4. Verify dependency resolution, jar placement, and the base Spark version. Run a connector class-loading or Kafka integration smoke check where the environment permits.

**Relevant files**
- `pom.xml` — dependency coordinates, repositories, and copied dependency output.
- `Dockerfile` — build stages, Spark classpath placement, and image checks.
- `.gitlab-ci.yml` — Kaniko build and GitLab Registry tags.

**Verification**
1. Resolve and copy dependencies with Maven; confirm all five requested artifacts resolve.
2. Build with Kaniko; confirm the jars are under `$SPARK_HOME/jars` and Spark reports version 4.1.3.
3. Check Kafka connector class loading or integration behavior when runtime infrastructure is available.
4. Confirm CI publishes the commit-SHA tag and updates `latest` only on the default branch.

**Decisions**
- Preserve the exact requested versions, including the Spark Kafka connector at 4.2.0. Its compatibility with the Spark 4.1.3 base is a runtime risk to verify.
- Use Kaniko and GitLab Container Registry.
- Add jars only; no application files, Python packages, or custom Spark configuration.
- The workspace is empty, so the plan introduces the Maven, Docker, and GitLab CI files without relying on existing project conventions.
