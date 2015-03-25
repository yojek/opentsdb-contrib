package net.opentsdb.kafka.consumer.parsers

import java.util

import net.opentsdb.core.{Tags, IncomingDataPoint}

import scala.collection.immutable

/**
 *
 * Created by alex on 12/10/14.
 *
 */


class OpenTSDBParser extends Parser{
  override def parse(message: String): List[IncomingDataPoint] = {

    val words = Tags.splitString(message, ' ')

    val metric = words{0}
    val timestamp =  System.currentTimeMillis() //Tags.parseLong(words{1})
    val value = words{2}

    val tags: util.HashMap[String, String] = new util.HashMap[String, String]

    for (i <- 3 until words.length if !words(i).isEmpty) {
      Tags.parse(tags, words(i))
    }
    val dataPoint = new IncomingDataPoint(metric, timestamp, value, tags)

    immutable.List(dataPoint)
  }
}
