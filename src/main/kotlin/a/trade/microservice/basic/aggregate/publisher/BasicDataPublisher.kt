package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import kafka_message.StockAggregate
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable

class BasicDataPublisher private constructor(
    private val runtimeApi: RuntimeApi,
) : Lifecycle {

    private val taskLifecycleDelegate = TaskLifecycleDelegate(runtimeApi)
    private val IOExec get() = runtimeApi.getExecutorService(ExecutorContext.IO)
    private val ComputeExec get() = runtimeApi.getExecutorService(ExecutorContext.COMPUTE)
    private val DefaultExec get() = runtimeApi.getExecutorService(ExecutorContext.DEFAULT)
    private lateinit var aggregateDataFilesystemReader: AggregateDataFilesystemReader

    init {
        createNewAggredateReader()
        if (!aggregateDataFilesystemReader.hasNext()) {
            throw IllegalArgumentException("No data found in the specified directory.")
        }
    }

    private fun createNewAggredateReader() {
        aggregateDataFilesystemReader = AggregateDataFilesystemReader("data/minute_aggs_v1")
    }

    companion object {
        private var instance: BasicDataPublisher? = null

        @Synchronized
        fun getInstance(runtimeApi: RuntimeApi): BasicDataPublisher {
            if (instance == null) {
                instance = BasicDataPublisher(runtimeApi)
            }
            return instance!!
        }
    }

    override fun start() {
        taskLifecycleDelegate.task = publishAllTask()
        taskLifecycleDelegate.start()
    }

    override fun stop() {
        taskLifecycleDelegate.stop()
    }

    override fun isRunning(): Boolean {
        return taskLifecycleDelegate.isRunning()
    }

    private fun publishAllTask(): Callable<*> {
        return Callable {
            val readAggregateTasks = getReadAggregateTasks()
            val batchSize = 20
            for (batch in readAggregateTasks.chunked(batchSize)) {
                val futures = batch.map { IOExec.submit(it) }
                for (future in futures) {
                    val stockAggregates = future.get()
                    publishAggregates()
                }
            }
        }
    }

    private fun getReadAggregateTasks(): MutableList<Callable<List<StockAggregate>>> {
        val result = mutableListOf<Callable<List<StockAggregate>>>()
        while (aggregateDataFilesystemReader.hasNext()) {
            result.add(aggregateDataFilesystemReader.getBatchTaskTransformADay())
        }
        return result
    }


    private fun publishAggregates() {

    }
}