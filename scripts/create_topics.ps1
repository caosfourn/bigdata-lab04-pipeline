param(
    [string]$BootstrapServer = "localhost:9092",
    [string]$KafkaContainer = "cpg-kafka"
)

$topics = @("cpg.nodes", "cpg.edges", "cpg.metadata", "cpg.errors")
foreach ($topic in $topics) {
    docker exec $KafkaContainer kafka-topics `
        --bootstrap-server $BootstrapServer `
        --create `
        --if-not-exists `
        --topic $topic `
        --partitions 3 `
        --replication-factor 1
}

docker exec $KafkaContainer kafka-topics `
    --bootstrap-server $BootstrapServer `
    --list
