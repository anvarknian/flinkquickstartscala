package org.apache.flink



import java.util.Properties

import flink.ConfluentAvroDeserializationSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

object FlinkKafkaExample extends App {


  object FlinkKafka {

    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // get CLI parameters
    val parameters = ParameterTool.fromArgs(args)
    val topic = parameters.getRequired("topic")
    val groupId = parameters.get("group-id", "flink-kafka-consumer")
    val propertiesFile = parameters.getRequired("env")
    val envProperties = ParameterTool.fromPropertiesFile(propertiesFile)
    val schemaRegistryUrl = envProperties.getRequired("registry_url")
    val boostrapServers = envProperties.getRequired("brokers")
    val zookeeperConnect = envProperties.getRequired("zookeeper")

    // setup Kafka sink
    val deserSchema = new ConfluentAvroDeserializationSchema(schemaRegistryUrl)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", boostrapServers)
    kafkaProps.setProperty("zookeeper.connect", zookeeperConnect)
    kafkaProps.setProperty("group.id", groupId)


    val consumer = new FlinkKafkaConsumer08[String]( topic , deserSchema , kafkaProps)
    consumer.setStartFromGroupOffsets()  // the default behaviour

    val stream = env.addSource(consumer)

    val windowSize = 1 // number of days that the window use to group events
    val windowStep = 1 // window slides 1 day

    val reducedStream = stream
      .map(transaction => {
        transaction.numberOfTransactions = 1
        transaction
      })
      .groupBy("InternalEvent")// that groups transactions in the same group
      .timeWindow(Time.days(windowSize),Time.days(windowStep))
      .sum("numberOfTransactions");


  // Exporting result as a Gson / scala string template

    val streamFormatedAsJson = reducedStream.map(functionToParseDataAsJson)
    streamFormatedAsJson.sink(SinkToWriteYourData)


  }

}

