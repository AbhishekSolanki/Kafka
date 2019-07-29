/**
 * @author : Abhishek Solanki
 * @date : 2019-07-28
 * @description : kafka streams example in scala-maven
 * @notes : kafka version 2.3.0 for scala 2.12
 * 					include kafka-client v2.3.0, kafka-streams v2.3.0, kafka-streams-scala_2.12 v2.3.0, scala-library 2.12.8 
 **/
package main.scala


import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

object KStream {

	def main(args : Array[String]) : Unit = {
			println("starting kstream application")


			val config: Properties = {
					val p = new Properties()
							p.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamone")
							p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
							p
			}

			val builder = new StreamsBuilder()

					val textLines: KStream[String, String] = builder.stream[String, String]("kstreaminput1")
					// textLines.to("kstreamoutput1")

					// static routing start  
					val router : Array[KStream[String, String]] = textLines.branch(
							(k,v)=>v.split(",")(0).equals("SYS_ONE"),
							(k,v)=>v.split(",")(0).equals("SYS_TWO"),
							(k,v)=>v.split(",")(0).equals("SYS_THREE"),
							(k,v) => true
							)

					router(0).to("SYSTEM_ONE")
					router(1).to("SYSTEM_TWO")
					router(2).to("SYSTEM_THREE")
					router(3).to("SYSTEM_GENERAL")
					// static routing end

					val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
					streams.cleanUp()
					streams.start()

					// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
					sys.ShutdownHookThread {
						streams.close(Duration.ofSeconds(10))
					}
	}
}