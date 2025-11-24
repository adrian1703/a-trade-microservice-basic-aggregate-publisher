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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@ThreadSafe
class BasicDataPublisher private constructor(
    private val runtimeApi: RuntimeApi,
) : Lifecycle {

    private val logger: Logger = LoggerFactory.getLogger(BasicDataPublisher::class.java)
    private val asyncTaskManager = AsyncTaskManager(runtimeApi)
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

    override fun start() {
        logger.info("Starting BasicDataPublisher")
        asyncTaskManager.task = publishAllTask()
        asyncTaskManager.start()
    }

    override fun stop() {
        logger.info("Stopping BasicDataPublisher")
        asyncTaskManager.stop()
    }

    override fun isRunning(): Boolean {
        val running = asyncTaskManager.isRunning()
        return running
    }

    private fun publishAllTask(): Callable<*> {
        fun prepareTopic() {
            logger.info("Preparing topic: ${STOCKAGGREGATE_ALL_1_MINUTE.topicName()}")
            runtimeApi.messageApi.deleteTopic(listOf(STOCKAGGREGATE_ALL_1_MINUTE.topicName()))
            runtimeApi.messageApi.createTopic(listOf(STOCKAGGREGATE_ALL_1_MINUTE.topicName()))
            logger.info("Topic prepared: ${STOCKAGGREGATE_ALL_1_MINUTE.topicName()}")
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
                val batchSize = 20
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
                            .map { ProducerRecord(STOCKAGGREGATE_ALL_1_MINUTE.topicName(), it.ticker, it) }
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