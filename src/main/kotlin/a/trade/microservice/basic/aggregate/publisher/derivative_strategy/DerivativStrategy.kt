package a.trade.microservice.basic.aggregate.publisher.derivative_strategy

import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.*

abstract class DerivativStrategy(
    private val runtimeApi: RuntimeApi,
    val inputTopic: Topics.Instance,
    val outputTopic: Topics.Instance,
) {

    private val exec: ExecutorService get() = runtimeApi.getExecutorService(ExecutorContext.UNBOUNDED)
    private val buffers = ConcurrentHashMap<String, BlockingQueue<*>>()
    private val futures = mutableListOf<Future<*>>()

    protected fun <T> createBuffer(name: String): LinkedBlockingQueue<T> {
        val result = LinkedBlockingQueue<T>(5000)
        buffers.put(name, result)
        return result
    }

    fun execute() {
        val tasks = configureTasks(inputTopic, outputTopic)
        tasks.forEach { futures.add(exec.submit(it)) }
        val monitorTask = createMonitorTask(
            buffers,
            futures,
            LoggerFactory.getLogger(this::class.java),
        )
        exec.submit { monitorTask }
    }

    protected abstract fun configureTasks(inputTopic: Topics.Instance, outputTopic: Topics.Instance): List<Callable<*>>

    private fun createMonitorTask(
        buffers: Map<String, BlockingQueue<*>>,
        futures: List<Future<*>>,
        logger: Logger,
        intervalMillis: Long = 5000,
    ): Runnable {
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