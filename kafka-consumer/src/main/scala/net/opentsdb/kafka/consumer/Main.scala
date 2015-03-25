package net.opentsdb.kafka.consumer

import org.slf4j.LoggerFactory
import java.io.{FileInputStream, BufferedInputStream, File}
import java.util.Properties
import com.google.inject.Guice
import net.opentsdb.kafka.consumer.modules.ConsumerModule

class Main {}

object Main {
  private final val logger = LoggerFactory.getLogger(classOf[Main])

  def main(args: Array[String]) {
    val props = loadProps(new File(args{0}))

    val injector = Guice.createInjector(new ConsumerModule(args{0}, props))
    logger.info("Starting TSDB Consumer...")
    val consumer = injector.getInstance(classOf[TsdbConsumer])
    consumer.startAsync()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        logger.info("Shutting down consumer")
        consumer.stopAsync()
      }
    })
  }

  private def loadProps(config: File): Properties = {
    if(!config.canRead) {
      System.err.println("Cannot open config file: " + config.getAbsolutePath)
      System.exit(1)
    }

    val props = new Properties()
    props.load(new BufferedInputStream(new FileInputStream(config)))

    logger.info("Loadded the file properties {}", props.toString)

    props
  }
}
