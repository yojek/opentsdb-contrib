package net.opentsdb.kafka.consumer.parsers

import java.util
import scala.util.control.Breaks._
import net.opentsdb.core.{IncomingDataPoint, Tags}

/**
 *
 * Created by alex on 12/11/14.
 */


class SFlowDatagramParser extends Parser {

  var LINES_DELIMITER: String = "@@@ "

  var metadata_key_filter: util.Set[String] = new util.HashSet[String](util.Arrays.asList(
    "samplesInPacket",
    "sysUpTime",
    "packetSequenceNo",
    "agent",
    "agentSubId",
    "datagramVersion",
    "datagramSize"))

  var metrics_key_filter: util.Set[String] = new util.HashSet[String](util.Arrays.asList(
    "sampleType_tag",
    "sampleType",
    "sampleSequenceNo",
    "sourceId",
    "counterBlock_tag",
    "adaptor_0_ifIndex",
    "adaptor_0_MACs",
    "adaptor_0_MAC_0",
    "adaptor_1_ifIndex",
    "adaptor_1_MACs",
    "adaptor_1_MAC_0",
    "adaptor_2_ifIndex",
    "adaptor_2_MACs",
    "adaptor_2_MAC_0",
    "counterBlock_tag",
    "hostname", "UUID",
    "machine_type",
    "os_name",
    "os_release",
    "parent_dsClass",
    "parent_dsIndex"
  ))

  override def parse(message: String): List[IncomingDataPoint] = {

    val lines = message.split(LINES_DELIMITER)

    var dataPoints: List[IncomingDataPoint] = List[IncomingDataPoint]()

    var timestamp: Long = 0L
    var tags: util.HashMap[String, String] = new util.HashMap[String, String]
    var metrics: util.HashMap[String, String] = new util.HashMap[String, String]

    var section = ""

    for (i <- 0 until lines.length if !lines{i}.isEmpty) {

      breakable {
        val line = lines {i}

        if (line.startsWith("startDatagram ")) {
          section = "metadata"
          break()
        } else if (line.startsWith("startSample ")) {
          section = "sample"
          break()
        } else if (line.startsWith("endSample ")) {
          section = ""
          break()
        } else if (line.startsWith("startTags")) {
          section = "tags"
          break()
        } else if (line.startsWith("endTags ")) {
          section = ""
          break()
        } else if (line.startsWith("endDatagram ")) {
          section = ""
          //add all data points
          val iter = metrics.keySet().iterator()

          while (iter.hasNext) {
            val metricName = iter.next()
            val currTags = new util.HashMap[String, String](tags)
            val point = new IncomingDataPoint(metricName, timestamp, metrics.get(metricName), currTags)
            dataPoints = point :: dataPoints

          }
          // clear aggregators
          metrics.clear()
          tags.clear()
          timestamp = 0L

          break()
        }

        if(section.isEmpty) {
          break()
        }

        val temp = line.split(" ")
        val key = temp{0}
        val value = temp{1}

        if (section.equals("metadata")) {
          if(metadata_key_filter.contains(key)) {
            break()
          }
          if(key.equals("unixSecondsUTC")) {
            timestamp = Tags.parseLong(value)
          } else {
            tags.put(key,value)
          }
        } else if(section.equals("sample")) {
          if(metrics_key_filter.contains(key)) {
            break()
          }
          metrics.put(key,value)
        } else if(section.equals("tags")) {
          tags.put(key,value)
        }
      }

    }

    dataPoints
  }

}

