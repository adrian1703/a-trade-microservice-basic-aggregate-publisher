package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.consumers

import java.util.concurrent.BlockingQueue

interface Consumer<T> {
    fun consumeFrom(buffer: BlockingQueue<T>)
}