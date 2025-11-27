package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.transformers

import java.util.concurrent.BlockingQueue

interface Transformer<IN, OUT> {
    fun transform(input: BlockingQueue<IN>, output: BlockingQueue<OUT>)
}