package a.trade.microservice.basic.aggregate.publisher

import a.trade.microservice.runtime_api.AsyncTaskManager
import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics.Instances.STOCKAGGREGATE_ALL_1_MINUTE
import kafka_message.StockAggregate
import net.jcip.annotations.ThreadSafe
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable
import org.apache.kafka.clients.producer.ProducerRecord

@ThreadSafe
class BasicDataPublisher private constructor(
    private val runtimeApi: RuntimeApi,
) : Lifecycle {

    private val asyncTaskManager = AsyncTaskManager(runtimeApi)
    private val ioExec get() = runtimeApi.getExecutorService(ExecutorContext.IO)
    private lateinit var aggregateDataFilesystemReader: AggregateDataFilesystemReader

    init {
        createNewAggregateReader()
        if (!aggregateDataFilesystemReader.hasNext()) {
            throw IllegalArgumentException("No data found in the specified directory.")
        }
    }

    private fun createNewAggregateReader() {
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
        asyncTaskManager.task = publishAllTask()
        asyncTaskManager.start()
    }

    override fun stop() {
        asyncTaskManager.stop()
    }

    override fun isRunning(): Boolean {
        return asyncTaskManager.isRunning()
    }

    private fun publishAllTask(): Callable<*> {
        fun prepareTopic() {
            runtimeApi.messageApi.deleteTopic(listOf(STOCKAGGREGATE_ALL_1_MINUTE.topicName()))
            runtimeApi.messageApi.createTopic(listOf(STOCKAGGREGATE_ALL_1_MINUTE.topicName()))
        }

        fun getReadAggregateTasks(): List<Callable<List<StockAggregate>>> {
            val result = mutableListOf<Callable<List<StockAggregate>>>()
            while (aggregateDataFilesystemReader.hasNext()) {
                result.add(aggregateDataFilesystemReader.getBatchTaskTransformADay())
            }
            return result
        }

        fun publishAggregatesInBatches(readAggregateTasks: List<Callable<List<StockAggregate>>>) {
            val producer = runtimeApi.messageApi.createAvroProducer<StockAggregate>()
            val batchSize = 20
            for (batch in readAggregateTasks.chunked(batchSize)) {
                val futures = batch.map { ioExec.submit(it) }
                for (future in futures) {
                    val stockAggregates = future.get()
                    stockAggregates
                        .map { ProducerRecord(STOCKAGGREGATE_ALL_1_MINUTE.topicName(), it.ticker, it) }
                        .forEach { producer.send(it) }
                }
            }
        }

        return Callable {
            prepareTopic()
            val readAggregateTasks = getReadAggregateTasks()
            publishAggregatesInBatches(readAggregateTasks)
        }
    }
}