package net.opentsdb.kafka.consumer.parsers

import net.opentsdb.core.IncomingDataPoint


/**
 *
 * Created by alex on 12/10/14.
 *
 */

trait Parser {
  def parse(message: String): List[IncomingDataPoint]
}
