package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.producers

import java.util.concurrent.BlockingQueue

interface Producer<T> {
    fun pushInto(buffer: BlockingQueue<T>)
}