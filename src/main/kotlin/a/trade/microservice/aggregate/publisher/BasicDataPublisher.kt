package a.trade.microservice.aggregate.publisher

import a.trade.microservice.runtime_api.ExecutorContext
import a.trade.microservice.runtime_api.RuntimeApi
import a.trade.microservice.runtime_api.Topics.Instance.STOCKAGGREGATE_ALL_1_MINUTE
import kafka_message.StockAggregate
import net.jcip.annotations.ThreadSafe
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.Lifecycle
import java.util.concurrent.Callable

@ThreadSafe
class BasicDataPublisher private constructor(
    runtimeApi: RuntimeApi,
    logger: Logger = LoggerFactory.getLogger(BasicDataPublisher::class.java),
) : Lifecycle, AbstractPublisher(runtimeApi, logger) {

    private val computeExec get() = runtimeApi.getExecutorService(ExecutorContext.COMPUTE)
    private lateinit var aggregateDataFilesystemReader: AggregateDataFilesystemReader

    init {
        logger.info("Initializing BasicDataPublisher")
        createNewAggregateReader()
        if (!aggregateDataFilesystemReader.hasNext()) {
            logger.error("No data found in the specified directory for AggregateDataFilesystemReader initialization.")
            throw IllegalArgumentException("No data found in the specified directory.")
        }
        logger.info("BasicDataPublisher initialized successfully with data available.")
    }

    private fun createNewAggregateReader() {
        logger.info("Creating new AggregateDataFilesystemReader for directory: data/minute_aggs_v1")
        aggregateDataFilesystemReader = AggregateDataFilesystemReader("data/minute_aggs_v1")
    }

    companion object {
        private var instance: BasicDataPublisher? = null

        @Synchronized
        fun getInstance(runtimeApi: RuntimeApi): BasicDataPublisher {
            if (instance == null) {
                logger.info("Creating a new instance of BasicDataPublisher")
                instance = BasicDataPublisher(runtimeApi)
            } else {
                logger.info("Returning existing instance of BasicDataPublisher")
            }
            return instance!!
        }

        private val logger: Logger = LoggerFactory.getLogger(BasicDataPublisher::class.java)
    }

    override fun taskToRun(): Callable<*> {
        return publishAllTask()
    }

    private fun publishAllTask(): Callable<*> {
        fun prepareTopic() {
            runtimeApi.messageApi.recreateTopic(listOf(STOCKAGGREGATE_ALL_1_MINUTE.rootName()))
        }

        fun getReadAggregateTasks(): List<Callable<List<StockAggregate>>> {
            val result = mutableListOf<Callable<List<StockAggregate>>>()
            var batches = 0
            while (aggregateDataFilesystemReader.hasNext()) {
                result.add(aggregateDataFilesystemReader.getBatchTaskTransformADay())
                batches++
            }
            logger.info("Prepared $batches aggregate read tasks from filesystem.")
            return result
        }

        fun publishAggregatesInBatches(readAggregateTasks: List<Callable<List<StockAggregate>>>) {
            runtimeApi.messageApi.createAvroProducer<StockAggregate>().use { producer ->
                val batchSize = 3
                var totalPublished = 0
                logger.info("Publishing aggregates in batches of $batchSize")
                for (batch in readAggregateTasks.chunked(batchSize)) {
                    logger.info("Submitting batch of size ${batch.size} for execution.")
                    val futures = batch.map { computeExec.submit(it) }
                    for (future in futures) {
                        val stockAggregates = future.get()
                        totalPublished += stockAggregates.size
                        logger.info("Publishing results for a total of $totalPublished.")
                        stockAggregates
                            .map { ProducerRecord(STOCKAGGREGATE_ALL_1_MINUTE.topicNameFor(it.ticker), it.ticker, it) }
                            .forEach { producer.send(it) }
                    }
                }
                logger.info("Published a total of $totalPublished StockAggregate records.")
            }
        }

        return Callable {
            logger.info("publishAllTask started")
            prepareTopic()
            val readAggregateTasks = getReadAggregateTasks()
            publishAggregatesInBatches(readAggregateTasks)
            logger.info("publishAllTask completed")
            return@Callable 0
        }
    }
}