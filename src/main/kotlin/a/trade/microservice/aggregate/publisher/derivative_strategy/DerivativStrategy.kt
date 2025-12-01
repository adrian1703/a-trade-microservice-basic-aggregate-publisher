package a.trade.microservice.aggregate.publisher.derivative_strategy

import a.trade.microservice.aggregate.publisher.derivative_strategy.consumers.WriteTopic
import a.trade.microservice.aggregate.publisher.derivative_strategy.producers.ReadTopic
import a.trade.microservice.aggregate.publisher.derivative_strategy.transformers.ProducerRecordMapper
import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.*

abstract class DerivativStrategy(
    private val runtimeApi: RuntimeApi,
    val inputTopic: Topics.Instance,
    val outputTopic: Topics.Instance,
) {

    private val exec: ExecutorService get() = runtimeApi.getExecutorService(ExecutorContext.COMPUTE)
    private val buffers = ConcurrentHashMap<String, BlockingQueue<*>>()
    private val futures = mutableListOf<Future<*>>()
    private val tasks = mutableListOf<Callable<*>>()

    protected fun <T> createBuffer(name: String, length: Int = 100): LinkedBlockingQueue<T> {
        val result = LinkedBlockingQueue<T>(length)
        buffers.put(name, result)
        return result
    }

    fun execute() {
        tasks.addAll(configureTasks(inputTopic, outputTopic))
        tasks.forEach { futures.add(exec.submit(it)) }
        val monitorTask = createMonitorTask(
            buffers,
            futures,
        )
//        exec.submit { monitorTask }
        monitorTask.run()
    }

    /**
     * Configures a list of tasks, each represented as a `Callable`. These tasks are designed
     * to process data by utilizing the input and output topics provided. The configuration might
     * involve setting up buffers, transformers, and data pipelines to handle specific operations.
     *
     * Example usage:
     * ```
     * val inputBuffer = preconfigureDefaultInput()
     * val orderFilter = StrictOrderFilter()
     * val orderFilterOutBuffer = createBuffer<StockAggregate>("OrderFilter-Out-Buffer")
     * preconfigureDefaultOutput(orderFilterOutBuffer)
     * return listOf(
     *     Callable { orderFilter.transform(inputBuffer, orderFilterOutBuffer) }
     * )
     * ```
     *
     * @param inputTopic the topic instance to consume input data from
     * @param outputTopic the topic instance to publish output data to
     * @return list of tasks representing processing steps in the pipeline
     */
    protected abstract fun configureTasks(inputTopic: Topics.Instance, outputTopic: Topics.Instance): List<Callable<*>>

    /**
     * Configures and initializes the default input pipeline for processing stock aggregate data.
     * This setup includes creating a buffer and linking it with a read operation on the input topic.
     * A background task is added to continuously read data from the input topic and populate the buffer.
     *
     * @return A blocking queue that serves as the output buffer for stock aggregate data read from the input topic.
     */
    protected fun preconfigureDefaultInput(): BlockingQueue<StockAggregate> {
        val input = ReadTopic(runtimeApi, inputTopic, inputTopic.rootName() + "-reader")
        val inputOutBuffer = createBuffer<StockAggregate>("Input-Out-Buffer")
        tasks.add(Callable { input.pushInto(inputOutBuffer) })
        return inputOutBuffer
    }

    /**
     * Prepares the default output process for handling and writing stock aggregate data.
     * It initializes the components required to map stock aggregates to producer records
     * and subsequently write them to the specified output topic.
     *
     * @param recordProducerInputBuffer The input buffer containing stock aggregate data
     * from which records are consumed, transformed, and routed for output processing.
     */
    protected fun preconfigureDefaultOutput(recordProducerInputBuffer: BlockingQueue<StockAggregate>) {
        val recordMapper = ProducerRecordMapper(outputTopic)
        val recordMapperOutBuffer = createBuffer<ProducerRecord<String, StockAggregate>>("Recordmapper-Output")
        val output = WriteTopic(runtimeApi)
        tasks.add(Callable { recordMapper.transform(recordProducerInputBuffer, recordMapperOutBuffer) })
        tasks.add(Callable { output.consumeFrom(recordMapperOutBuffer) })
    }

    private fun createMonitorTask(
        buffers: Map<String, BlockingQueue<*>>,
        futures: List<Future<*>>,
        intervalMillis: Long = 5000,
    ): Runnable {
        val logger = LoggerFactory.getLogger("MonitorTask-$intervalMillis")
        fun printBufferStatus() {
            logger.info("[Monitor] #########################################")
            buffers.entries.forEach { (name, buffer) ->
                logger.info("[Monitor] Buffer status: $name(size=${buffer.size})")
            }
        }

        fun checkFutureFailures(): Boolean {
            for (future in futures) {
                if (future.isDone) {
                    try {
                        future.get()
                    } catch (e: Exception) {
                        logger.error("[Monitor] FAILURE detected in worker: ${e.message}", e)
                        futures.forEach { f ->
                            if (!f.isDone) f.cancel(true)
                        }
                        logger.info("[Monitor] Cancelled all other threads due to failure.")
                        return true
                    }
                }
            }
            return false
        }

        val monitorTask = Runnable {
            try {
                val start = System.currentTimeMillis()
                while (true) {
                    printBufferStatus()
                    if (checkFutureFailures()) return@Runnable
                    if (futures.all { it.isDone }) {
                        val end = System.currentTimeMillis()
                        logger.info("[Monitor] All threads have finished! Exiting monitor. Total time: ${(end - start) / 1000}s.")
                        return@Runnable
                    }
                    Thread.sleep(intervalMillis)
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                logger.warn("[Monitor] Monitor interrupted, exiting.")
            }

        }
        return monitorTask
    }

}