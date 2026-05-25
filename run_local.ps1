pwsh -NoProfile -Command {
    $env:AXTEROID_LOG_LEVEL = "DEBUG"
    $env:AXTEROID_PLATFORM = "axt"
    $env:AXTEROID_STAGE = "prod"
    $env:AWS_REGION = "us-east-1"
    $env:DYNAMODB_URL = "https://dynamodb.$($env:AWS_REGION).amazonaws.com"
    $env:OPENSEARCH_IDX_URL = "http://os-idx-node1.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:9200,http://os-idx-node2.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:9200,http://os-idx-node3.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:9200"
    $env:OPENSEARCH_DS_URL = "http://opensearch-ds-srv.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:9201"
    $env:S3_URL = "https://s3.$($env:AWS_REGION).amazonaws.com"
    $env:KAFKA_BOOTSTRAP_SERVERS = "kafka-srv-node1.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:9092,kafka-srv-node2.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:9092,kafka-srv-node3.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:9092"
    $env:SCHEMA_REGISTRY_URL = "http://schema-registry-srv.$($env:AXTEROID_PLATFORM)-$($env:AXTEROID_STAGE).ecs.services:8081"


    java -cp .\target\cnsmr-sunat-val-injector-1.0.0.jar  com.axt.cnsmr.sunat.val.injector.Injector $args
} -Args $args