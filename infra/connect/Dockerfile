FROM confluentinc/cp-kafka-connect-base:7.8.0

ARG NEO4J_CONNECTOR_VERSION=5.5.0
ARG NEO4J_CONNECTOR_SHA256=b40cdd1d498c9d688af7580a32e8cc50c000d5d134051b60821cc24c5f3c7d68

USER root
RUN curl -fsSL --retry 5 \
      "https://github.com/neo4j/neo4j-kafka-connector/releases/download/${NEO4J_CONNECTOR_VERSION}/neo4j-kafka-connect-${NEO4J_CONNECTOR_VERSION}.zip" \
      -o /tmp/neo4j-kafka-connect.zip \
    && echo "${NEO4J_CONNECTOR_SHA256}  /tmp/neo4j-kafka-connect.zip" | sha256sum -c - \
    && mkdir -p /usr/share/confluent-hub-components \
    && cd /usr/share/confluent-hub-components \
    && jar xf /tmp/neo4j-kafka-connect.zip \
    && rm /tmp/neo4j-kafka-connect.zip \
    && chown -R appuser:appuser /usr/share/confluent-hub-components
USER appuser
