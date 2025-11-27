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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor

class AllToSingleStrategy(private val runtimeApi: RuntimeApi) {
    companion object {
        val inOut = Pair(Topics.Instance.STOCKAGGREGATE_ALL_1_MINUTE, Topics.Instance.STOCKAGGREGATE_SINGLE_1_MINUTE)
    }

    val exec: ExecutorService get() = runtimeApi.getExecutorService(ExecutorContext.UNBOUNDED)
    var buffers = ConcurrentHashMap<String, BlockingQueue<*>>()

    private fun <T> createBuffer(name: String): LinkedBlockingQueue<T> {
        val result = LinkedBlockingQueue<T>(10000)
        buffers.put(name, result)
        return result
    }

    fun execute() {
        val futures = mutableListOf<Future<*>>()
        buffers = ConcurrentHashMap<String, BlockingQueue<*>>()

        val input = ReadTopic(runtimeApi, inOut.first, pollDurationMillis = 2000)
        val inputTickerStrickOrderFilterBuffer = createBuffer<StockAggregate>("Input-OrderFilter")
        val orderFilter = StrictOrderFilter()
        val orderfilternRecordmapperBuffer = createBuffer<StockAggregate>("OrderFilter-Recordmapper")
        val recordMapper = ProducerRecordMapper(inOut.second)
        val recordMapperOutputBuffer = createBuffer<ProducerRecord<String, StockAggregate>>("Recordmapper-Output")
        val output = WriteTopic(runtimeApi)

        futures.add(exec.submit { input.pushInto(inputTickerStrickOrderFilterBuffer) })
        futures.add(exec.submit { orderFilter.transform(inputTickerStrickOrderFilterBuffer, orderfilternRecordmapperBuffer) })
        futures.add(exec.submit { recordMapper.transform(orderfilternRecordmapperBuffer, recordMapperOutputBuffer) })
        futures.add(exec.submit { output.consumeFrom(recordMapperOutputBuffer) })

        val monitorTask = createMonitorTask(
            buffers,
            futures,
            LoggerFactory.getLogger(this::class.java),
        )
        println("hello")
        monitorTask.run()
    }

    private fun createMonitorTask(
        buffers: Map<String, BlockingQueue<*>>,
        futures: List<Future<*>>,
        logger: org.slf4j.Logger,
        intervalMillis: Long = 5000,
    ): Runnable {
        fun printBufferStatus() {
            logger.info("[Monitor] #########################################")
            val executor = exec as ThreadPoolExecutor
            logger.info("[Monitor] Active threads: ${executor.activeCount}, corePoolSize: ${executor.corePoolSize}")
            logger.info("[Monitor] Completed tasks: ${executor.completedTaskCount}, taskCount: ${executor.taskCount}")
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
                while (true) {
                    printBufferStatus()
                    if (checkFutureFailures()) return@Runnable
                    if (futures.all { it.isDone }) {
                        logger.info("[Monitor] All threads have finished! Exiting monitor.")
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