package a.trade.microservice.aggregate.publisher.derivative_strategy

import a.trade.microservice.aggregate.publisher.derivative_strategy.transformers.StrictOrderFilter
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import java.util.concurrent.Callable

class AllToSingleStrategy(
    private val runtimeApi: RuntimeApi,
    inputTopic: Topics.Instance,
    outputTopic: Topics.Instance,
) : DerivativStrategy(runtimeApi, inputTopic, outputTopic) {
    override fun configureTasks(
        inputTopic: Topics.Instance,
        outputTopic: Topics.Instance,
    ): List<Callable<*>> {
        val inputOutBuffer = preconfigureDefaultInput()
        val orderFilter = StrictOrderFilter()
        val orderfilternRecordmapperBuffer = createBuffer<StockAggregate>("OrderFilter-Out-Buffer")
        preconfigureDefaultOutput(orderfilternRecordmapperBuffer)


        return listOf(
            Callable { orderFilter.transform(inputOutBuffer, orderfilternRecordmapperBuffer) },
        )
    }
}