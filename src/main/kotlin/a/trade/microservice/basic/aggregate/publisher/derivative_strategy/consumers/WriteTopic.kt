package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.consumers

import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.consumers.Consumer
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.StopCase
import a.trade.microservice.runtime_api.RuntimeApi
import kafka_message.StockAggregate
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

/**
 * Singleton, multiple instance will fail on the same input-buffer
 */
class WriteTopic(
    private val runtimeApi: RuntimeApi,
    private val recreateTopic: Boolean = true,
    private val seenTopics: HashSet<String> = HashSet(),
): Consumer<ProducerRecord<String, StockAggregate>> {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun consumeFrom(buffer: BlockingQueue<ProducerRecord<String, StockAggregate>>) {
        logger.info("Starting WriteTopic")
        runtimeApi.messageApi.createAvroProducer<StockAggregate>().use { producer ->
            while (true) {
                val record = buffer.take()
                if (recreateTopic && !seenTopics.contains(record.topic())) {
                    runtimeApi.messageApi.recreateTopic(listOf(record.topic()))
                    seenTopics.add(record.topic())
                }
                if (record.value() is StopCase) {
                    logger.info("StopCase received, exiting.")
                    break
                }
                producer.send(record)
            }
        }
    }
}