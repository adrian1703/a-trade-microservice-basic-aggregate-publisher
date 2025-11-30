package a.trade.microservice.basic.aggregate.publisher.derivative_strategy

import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.consumers.WriteTopic
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.producers.ReadTopic
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.transformers.ProducerRecordMapper
import a.trade.microservice.basic.aggregate.publisher.derivative_strategy.transformers.WindowAggregator
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import org.apache.kafka.clients.producer.ProducerRecord
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
        val input = ReadTopic(runtimeApi, inputTopic, inputTopic.rootName() + "-reader")
        val inputOutBuffer = createBuffer<StockAggregate>("Input-OrderFilter")
        val windowAggregator = WindowAggregator(inputTopic, outputTopic)
        val windowAggregatorOut = createBuffer<StockAggregate>("WindowAggregator-Recordmapper")
        val recordMapper = ProducerRecordMapper(outputTopic)
        val recordMapperOutBuffer = createBuffer<ProducerRecord<String, StockAggregate>>("Recordmapper-Output")
        val output = WriteTopic(runtimeApi)

        return listOf(
            Callable { input.pushInto(inputOutBuffer) },
            Callable { windowAggregator.transform(inputOutBuffer, windowAggregatorOut) },
            Callable { recordMapper.transform(windowAggregatorOut, recordMapperOutBuffer) },
            Callable { output.consumeFrom(recordMapperOutBuffer) },
        )
    }
}