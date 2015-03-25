package net.opentsdb.kafka.consumer.modules

import com.google.inject.{Singleton, Provides, AbstractModule}
import net.codingwell.scalaguice.ScalaModule
import java.util.Properties
import com.google.inject.name. Names
import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config

class ConsumerModule(propsPath: String, props:Properties) extends AbstractModule with ScalaModule {
  def configure() {
    Names.bindProperties(binder(), props)
  }

  @Provides @Singleton def provideProperties: Properties = props
  @Provides @Singleton def providePropsPath: String = propsPath

  @Provides @Singleton def provideTsdbClient(propsPath: String): TSDB = {
    val config = new Config(propsPath)

    val client = new TSDB(config)

    client
  }

}
