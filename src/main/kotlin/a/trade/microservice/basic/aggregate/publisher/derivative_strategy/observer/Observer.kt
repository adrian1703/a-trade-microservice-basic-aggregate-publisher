package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.observer

fun interface Observer<T> {
    fun onUpdate(event: T)
}