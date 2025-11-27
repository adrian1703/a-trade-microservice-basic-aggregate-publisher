package a.trade.microservice.basic.aggregate.publisher.derivative_strategy.observer

open class EventSource<T> {
    private val observers = mutableSetOf<Observer<T>>()

    fun addObserver(observer: Observer<T>) {
        observers += observer
    }

    protected fun notifyObservers(event: T) {
        observers.forEach { it.onUpdate(event) }
    }
}
