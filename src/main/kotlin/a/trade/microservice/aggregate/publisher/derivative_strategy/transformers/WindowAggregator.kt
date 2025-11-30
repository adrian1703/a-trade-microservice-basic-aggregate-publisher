package a.trade.microservice.aggregate.publisher.derivative_strategy.transformers

import a.trade.microservice.aggregate.publisher.derivative_strategy.specialcase.StopCase
import a.trade.microservice.runtime_api.Topics
import kafka_message.StockAggregate
import org.slf4j.LoggerFactory
import java.util.concurrent.BlockingQueue

class WindowAggregator(
    val inputTopic: Topics.Instance,
    val outputTopics: Topics.Instance,
) : Transformer<StockAggregate, StockAggregate> {
    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * Maintains the last windowed aggregate for each ticker symbol.
     *
     * Structure:
     * - Key: Ticker symbol as a String.
     * - Value: Pair where:
     *      - first: Timestamp (Long) representing the start of the last aggregation window processed for this ticker.
     *      - second: The corresponding StockAggregate that was received for that window.
     *
     * Usage:
     * - For each incoming StockAggregate, this map allows for a quick lookup of the last processed aggregate for the ticker.
     * - This helps ensure that only aggregates with strictly increasing windowStart timestamps are processed and output,
     *   effectively filtering out unordered or duplicate aggregates for the same ticker.
     *
     * Window logic:
     *   For a given ticker, StockAggregates are output only if their windowStart is strictly greater than the last seen.
     *   Aggregates in the time interval (last windowStart, last windowStart + outputTopic.durationInMillis] for a ticker
     *   can be used for further rolling or windowed computations if needed.
     */

    private val lastWindowedAggregateForTicker = HashMap<String, Pair<Long, MutableList<StockAggregate>>>()

    init {
        if (inputTopic.durationInMillis > outputTopics.durationInMillis) throw IllegalArgumentException("Input topic duration (${inputTopic.durationInMillis}) must be smaller or equal to output topic duration (${outputTopics.durationInMillis})")
    }

    override fun transform(input: BlockingQueue<StockAggregate>, output: BlockingQueue<StockAggregate>) {
        logger.info("Starting WindowAggregator")
        val targetDuration = outputTopics.durationInMillis
        while (true) {
            val stockAggregate = input.take()
            val currentTicker = stockAggregate.ticker
            val currentWindow = stockAggregate.windowStart
            // break condition
            if (stockAggregate is StopCase) {
                logger.info("StopCase received, exiting.")
                publishRemainingWindows(output)
                output.put(stockAggregate)
                break
            }
            // case 1: ticker in not yet present
            if (!lastWindowedAggregateForTicker.containsKey(currentTicker)) {
                initializeTickerFor(stockAggregate)
                break
            }
            // case 2: ticker is present and within target duration
            val lastWindow = lastWindowedAggregateForTicker[stockAggregate.ticker]!!.first
            if (currentWindow + targetDuration > lastWindow) {
                lastWindowedAggregateForTicker[stockAggregate.ticker]!!.second.add(stockAggregate)
                break
            }
            // case 3: ticker is present and is over target duration
            publishTickerAggregateFor(currentTicker, output)
            initializeTickerFor(stockAggregate)
        }
    }

    private fun initializeTickerFor(stockAggregate: StockAggregate) {
        lastWindowedAggregateForTicker[stockAggregate.ticker] =
            Pair(stockAggregate.windowStart, mutableListOf(stockAggregate))
    }

    private fun publishTickerAggregateFor(ticker: String, output: BlockingQueue<StockAggregate>) {
        val newAggregation = aggregateStockaggregates(lastWindowedAggregateForTicker[ticker]!!.second)
        output.put(newAggregation)
    }

    private fun publishRemainingWindows(output: BlockingQueue<StockAggregate>) {
        lastWindowedAggregateForTicker.entries.forEach { (key, value) ->
            publishTickerAggregateFor(key, output)
        }
        lastWindowedAggregateForTicker.clear()
    }

    private fun aggregateStockaggregates(aggregates: List<StockAggregate>): StockAggregate {
        val result = StockAggregate()
        result.ticker = aggregates.first().ticker
        result.windowStart = aggregates.first().windowStart
        result.volume = aggregates.sumOf { it.volume }
        result.open = aggregates.first().open
        result.high = aggregates.maxOf { it.high }
        result.low = aggregates.minOf { it.low }
        result.close = aggregates.last().close
        return result
    }

}