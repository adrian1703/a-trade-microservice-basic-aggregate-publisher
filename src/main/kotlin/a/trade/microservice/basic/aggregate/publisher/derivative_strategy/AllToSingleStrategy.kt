package a.trade.microservice.basic.aggregate.publisher.derivative_strategy

import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.consumers.WriteTopic
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.producers.ReadTopic
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.transformers.ProducerRecordMapper
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.transformers.StrictOrderFilter
import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor

class AllToSingleStrategy(private val runtimeApi: RuntimeApi) : DerivativStrategy(runtimeApi) {

    override fun configureTasks(
        inputTopic: Topics.Instance,
        outputTopic: Topics.Instance,
    ): List<Callable<*>> {
        val input = ReadTopic(runtimeApi, inputTopic, "all-to-single-5-min-reader")
        val inputTickerStrickOrderFilterBuffer = createBuffer<StockAggregate>("Input-OrderFilter")
        val orderFilter = StrictOrderFilter()
        val orderfilternRecordmapperBuffer = createBuffer<StockAggregate>("OrderFilter-Recordmapper")
        val recordMapper = ProducerRecordMapper(outputTopic)
        val recordMapperOutputBuffer = createBuffer<ProducerRecord<String, StockAggregate>>("Recordmapper-Output")
        val output = WriteTopic(runtimeApi)

        return listOf(
            Callable { input.pushInto(inputTickerStrickOrderFilterBuffer) },
            Callable { orderFilter.transform(inputTickerStrickOrderFilterBuffer, orderfilternRecordmapperBuffer) },
            Callable { recordMapper.transform(orderfilternRecordmapperBuffer, recordMapperOutputBuffer) },
            Callable { output.consumeFrom(recordMapperOutputBuffer) },
        )
    }
}