package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.specialcase

import kafka_message.StockAggregate

class StopCase: StockAggregate() {
    init {
        super.ticker = "STOP"
    }
}