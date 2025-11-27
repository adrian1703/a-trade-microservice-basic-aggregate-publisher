package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.AllToSingleStrategy
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap

class DerivedDataPublisher private constructor(
    runtimeApi: RuntimeApi,
    val inputTopic: Topics.Instance,
    val outputTopic: Topics.Instance,
    val strategy: AllToSingleStrategy,
    logger: Logger = LoggerFactory.getLogger(DerivedDataPublisher::class.java),
) : Lifecycle, AbstractPublisher(runtimeApi, logger) {

    companion object {
        private val instances: ConcurrentHashMap<Pair<Topics.Instance, Topics.Instance>, DerivedDataPublisher> =
            ConcurrentHashMap()
        private val factoryMethods: ConcurrentHashMap<Pair<Topics.Instance, Topics.Instance>, (RuntimeApi) -> AllToSingleStrategy> =
            ConcurrentHashMap()

        init {
            factoryMethods[AllToSingleStrategy.inOut] = { runtimeApi: RuntimeApi -> AllToSingleStrategy(runtimeApi) }
        }

        @Synchronized
        fun getInstance(
            runtimeApi: RuntimeApi,
            inputTopic: Topics.Instance,
            outputTopic: Topics.Instance,
        ): DerivedDataPublisher {
            val requested = Pair(inputTopic, outputTopic)
            if (instances.get(requested) == null) {
                logger.info("Creating a new instance")
                val strategy = factoryMethods[requested]!!.invoke(runtimeApi)
                instances[requested] =
                    DerivedDataPublisher(runtimeApi, inputTopic, outputTopic, strategy)
            } else {
                logger.info("Returning existing instance")
            }
            return instances[requested]!!
        }

        private val logger: Logger = LoggerFactory.getLogger(DerivedDataPublisher::class.java)

    }

    override fun taskToRun(): Callable<*> {
        return Callable { strategy.execute() }
    }
}