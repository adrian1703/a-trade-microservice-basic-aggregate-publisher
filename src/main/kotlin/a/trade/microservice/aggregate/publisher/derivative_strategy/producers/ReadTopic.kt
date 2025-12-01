package a.trade.microservice.aggregate.publisher.derivative_strategy.producers

import a.trade.microservice.aggregate.publisher.derivative_strategy.specialcase.StopCase
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.BlockingQueue

class ReadTopic(
    private val runtimeApi: RuntimeApi,
    private val topic: Topics.Instance,
    private val groupName: String = UUID.randomUUID().toString(),
    private val tickerName: String? = null,
    val pollDurationMillis: Long = 100,
) : Producer<StockAggregate> {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun pushInto(buffer: BlockingQueue<StockAggregate>) {
        logger.info("Starting ReadTopic")
        runtimeApi.messageApi.createAvroConsumer<StockAggregate>(groupName).use { consumer ->
            consumer.subscribe(listOf(topic.topicNameFor(tickerName)))
            while (true) {
                val records = consumer.poll(Duration.ofMillis(pollDurationMillis))
                if (records.isEmpty) {
                    logger.info("No records polled in this interval.")
                }
                records.forEach { buffer.put(it.value()) }
                if (runtimeApi.messageApi.lastRecordReached(consumer)) {
                    logger.info("Last record reached on topic: $topic, sending StopCase.")
                    buffer.put(StopCase())
                    break
                }
            }
        }
        runtimeApi.messageApi.deleteConsumerGroups(listOf(groupName))
        logger.info("ReadTopic finished.")
    }
}