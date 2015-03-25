package net.opentsdb.kafka.consumer

import com.google.inject.Inject
import com.google.common.util.concurrent.AbstractIdleService
import java.util.Properties
import com.google.inject.name.Named
import net.opentsdb.kafka.consumer.parsers.{SFlowDatagramParser, OpenTSDBParser}
import org.slf4j.LoggerFactory
import kafka.consumer.{ConsumerConfig, Consumer}
import java.util.concurrent.Executors
import javax.inject.Singleton
import kafka.serializer.{DefaultDecoder, StringDecoder}
import net.opentsdb.core.{IncomingDataPoint, Tags, TSDB}
import net.opentsdb.stats.StatsCollector
import java.lang.Float

@Singleton
class TsdbConsumer @Inject() (props: Properties, tsdbClient: TSDB, @Named("tsdb.kafka.topic") topicName: String,
                              @Named("tsdb.kafka.topic.partitions") partitions: Int) extends AbstractIdleService{
  private final val logger = LoggerFactory.getLogger(classOf[TsdbConsumer])
  private final val consumerConnector = Consumer.create(new ConsumerConfig(props))
  private final val executorPool = Executors.newFixedThreadPool(partitions, new ConsumerThreadFactory)


  def startUp() {

    tsdbClient.collectStats(new StatsCollector("tsd") {
      override def emit(line: String) = logger.info(line)
    })

    logger.info("Starting up TSDB Kafka Consumer")
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(topicName -> partitions),
      new DefaultDecoder,
      new StringDecoder())

    val parser = new SFlowDatagramParser
    val topicStreams = topicMessageStreams.get(topicName).get

    for(stream <- topicStreams) {
      executorPool.execute(new Runnable() {
        override def run() {
          for(kafkaMessage <- stream) {
            logger.info("New kafka message => " + kafkaMessage.message)

            val parsedMessages : List[IncomingDataPoint] = parser.parse(kafkaMessage.message())

            for(parsedMessage<-parsedMessages) {

              if (parsedMessage.getValue.equals("NaN") || parsedMessage.getValue.startsWith("N")) {
                tsdbClient.addPoint(parsedMessage.getMetric, parsedMessage.getTimestamp, 0, parsedMessage.getTags)
              } else if (Tags.looksLikeInteger(parsedMessage.getValue)) {
                tsdbClient.addPoint(parsedMessage.getMetric, parsedMessage.getTimestamp, Tags.parseLong(parsedMessage.getValue), parsedMessage.getTags)
              } else {
                tsdbClient.addPoint(parsedMessage.getMetric, parsedMessage.getTimestamp, Float.parseFloat(parsedMessage.getValue), parsedMessage.getTags)
              }
            }
          }
        }
      })
    }
  }

  def shutDown() {
    logger.info("Shutting down TSDB Kafka Consumer")
    tsdbClient.shutdown()
    executorPool.shutdown()
    consumerConnector.shutdown()
  }
}


