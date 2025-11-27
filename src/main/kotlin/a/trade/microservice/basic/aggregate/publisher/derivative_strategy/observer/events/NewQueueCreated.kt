package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.observer.events

import kafka_message.StockAggregate
import java.util.concurrent.BlockingQueue

class NewQueueCreated(val queueName: String, val queue: BlockingQueue<StockAggregate>) {
}