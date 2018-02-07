package org.apache.flink



import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08


object AggregationPilote extends App  {


    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    // get CLI parameters
    val parameters = ParameterTool.fromArgs(args)
    val topic = parameters.getRequired("topic")
    val groupId = parameters.get("group-id", "flink-kafka-consumer")
    val propertiesFile = parameters.getRequired("env")
    val envProperties = ParameterTool.fromPropertiesFile(propertiesFile)
    val boostrapServers = envProperties.getRequired("brokers")
    val zookeeperConnect = envProperties.getRequired("zookeeper")

    // setup Kafka sink

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", boostrapServers)
    kafkaProps.setProperty("zookeeper.connect", zookeeperConnect)
    kafkaProps.setProperty("group.id", groupId)

    val consumer = new FlinkKafkaConsumer08[String] (java.util.regex.Pattern.compile("day-[1-3]"), new SimpleStringSchema, kafkaProps)



    val stream = env.addSource(consumer)

    val windowSize = 1 // number of days that the window use to group events
    val windowStep = 1 // window slides 1 day

    val reducedStream = stream
      .keyBy("InternalEvent", _ )// groups transactions in the same group
      .timeWindow(Time.days(windowSize),Time.days(windowStep))
      .map(transaction => {
            transaction.numberOfTransactions = 1
            transaction
            })
      .sum("numberOfTransactions")

    env.execute()
    // Exporting result as a Gson / scala string template
    // val streamFormatedAsJson = reducedStream.map(functionToParseDataAsJson)
    // streamFormatedAsJson.sink(SinkToWriteYourData)


}

