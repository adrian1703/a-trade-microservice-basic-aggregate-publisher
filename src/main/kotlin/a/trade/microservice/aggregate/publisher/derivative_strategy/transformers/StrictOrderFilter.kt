package a.trade.microservice.aggregate.publisher.derivative_strategy.transformers

import a.trade.microservice.aggregate.publisher.derivative_strategy.specialcase.StopCase
import kafka_message.StockAggregate
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

class StrictOrderFilter : Transformer<StockAggregate, StockAggregate> {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private val lastAggregateForTicker = HashMap<String, StockAggregate>()

    override fun transform(input: BlockingQueue<StockAggregate>, output: BlockingQueue<StockAggregate>) {
        logger.info("Starting StrictOrderFilter")
        while (true) {
            val stockAggregate = input.take()
            if (stockAggregate is StopCase) {
                logger.info("StopCase received, exiting.")
                output.put(stockAggregate)
                break
            }
            val lastTimestamp = lastAggregateForTicker[stockAggregate.ticker]?.windowStart ?: 0L
            lastAggregateForTicker[stockAggregate.ticker] = stockAggregate
            if (stockAggregate.windowStart > lastTimestamp) {
                output.put(stockAggregate)
            } else {
                continue // drop unordered
            }
        }
    }
}