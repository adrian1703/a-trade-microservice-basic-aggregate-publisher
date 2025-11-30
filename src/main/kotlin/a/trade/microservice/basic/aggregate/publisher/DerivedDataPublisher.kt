package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.AllToSingleStrategy
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.DerivativStrategy
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.WindowAggregationStrategy
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable

class DerivedDataPublisher private constructor(
    runtimeApi: RuntimeApi,
    val inputTopic: Topics.Instance,
    val outputTopic: Topics.Instance,
    val strategy: DerivativStrategy,
    logger: Logger = LoggerFactory.getLogger(DerivedDataPublisher::class.java),
) : Lifecycle, AbstractPublisher(runtimeApi, logger) {

    companion object {

        @Synchronized
        fun getInstance(
            runtimeApi: RuntimeApi,
            inputTopic: Topics.Instance,
            outputTopic: Topics.Instance,
        ): DerivedDataPublisher {
            val strategy = selectStrategy(runtimeApi, inputTopic, outputTopic)
            val result = DerivedDataPublisher(runtimeApi, inputTopic, outputTopic, strategy)
            return result
        }

        private val logger: Logger = LoggerFactory.getLogger(DerivedDataPublisher::class.java)

        private fun selectStrategy(
            runtimeApi: RuntimeApi,
            inputTopic: Topics.Instance,
            outputTopic: Topics.Instance,
        ): DerivativStrategy {
            if (inputTopic.aggregateKind == Topics.AggregateKind.ALL && outputTopic.aggregateKind == Topics.AggregateKind.SINGLE) {
                return AllToSingleStrategy(runtimeApi, inputTopic, outputTopic)
            } else if (inputTopic.durationInMillis < outputTopic.durationInMillis) {
                return WindowAggregationStrategy(runtimeApi, inputTopic, outputTopic)
            }
            throw IllegalArgumentException("Unsupported combination of input and output topics: $inputTopic, $outputTopic")
        }
    }

    override fun taskToRun(): Callable<*> {
        return Callable { strategy.execute() }
    }
}