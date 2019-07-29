package main.scala

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

object Quickstart {

	def main(args : Array[String]) : Unit = {
			println("Quickstart example")

			val config: Properties = {
					val p = new Properties()
							p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamone")
							p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
							p
			}

			val builder = new StreamsBuilder()
      val textLines: KStream[String, String] = builder.stream[String, String]("kstreaminput")
		  textLines.to("kstreamoutput")

		  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
      streams.cleanUp()
      streams.start()
      sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
      }
	}
}