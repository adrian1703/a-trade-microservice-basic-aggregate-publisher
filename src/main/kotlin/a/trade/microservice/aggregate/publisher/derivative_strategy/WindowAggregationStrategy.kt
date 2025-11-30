package a.trade.microservice.aggregate.publisher.derivative_strategy

import a.trade.microservice.aggregate.publisher.derivative_strategy.transformers.WindowAggregator
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import java.util.concurrent.Callable

class WindowAggregationStrategy(
    private val runtimeApi: RuntimeApi,
    inputTopic: Topics.Instance,
    outputTopic: Topics.Instance,
) : DerivativStrategy(runtimeApi, inputTopic, outputTopic) {
    override fun configureTasks(
        inputTopic: Topics.Instance,
        outputTopic: Topics.Instance,
    ): List<Callable<*>> {
        val inputOutBuffer = preconfigureDefaultInput()
        val windowAggregator = WindowAggregator(inputTopic, outputTopic)
        val windowAggregatorOut = createBuffer<StockAggregate>("WindowAggregator-Out-Buffer")
        preconfigureDefaultOutput(windowAggregatorOut)

        return listOf(
            Callable { windowAggregator.transform(inputOutBuffer, windowAggregatorOut) },
        )
    }
}