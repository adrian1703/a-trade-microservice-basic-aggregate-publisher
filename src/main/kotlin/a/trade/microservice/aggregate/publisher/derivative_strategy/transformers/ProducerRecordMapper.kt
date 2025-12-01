package a.trade.microservice.aggregate.publisher.derivative_strategy.transformers

import a.trade.microservice.aggregate.publisher.derivative_strategy.specialcase.StopCase
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

class ProducerRecordMapper(private val topic: Topics.Instance) :
    Transformer<StockAggregate, ProducerRecord<String, StockAggregate>> {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun transform(
        input: BlockingQueue<StockAggregate>,
        output: BlockingQueue<ProducerRecord<String, StockAggregate>>,
    ) {
        logger.info("Starting ProducerRecordMapper")
        while (true) {
            val stockAggregate = input.take()
            val producerRecord =
                ProducerRecord<String, StockAggregate>(topic.topicNameFor(stockAggregate.ticker), stockAggregate)
            output.put(producerRecord)
            if (stockAggregate is StopCase) {
                logger.info("StopCase received, exiting.")
                break
            }
        }
        logger.info("ProducerRecordMapper finished.")
    }
}