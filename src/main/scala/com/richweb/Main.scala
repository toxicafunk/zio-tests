package com.richweb

import zio.{App, DefaultRuntime, Runtime, Task, UIO, ZIO}
import zio.console.putStrLn
import zio.stream._
import zio.blocking.Blocking
import zio.console.Console

import zio.metrics._
import zio.metrics.prometheus._
import zio.metrics.prometheus.helpers._

import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse

import com.richweb.filereader._
import com.richweb.messaging.Messaging

import cakesolutions.kafka.KafkaProducer.Conf
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.RecordMetadata

import zio.internal.PlatformLive
import com.richweb.messaging.MessagingLive

import org.apache.kafka.common.serialization.StringSerializer
import scala.math.Numeric.IntIsIntegral

import java.net.InetSocketAddress

object Main {

  val toJson: String => Json = txt =>
    parse(txt) match {
      case Right(j)      => j
      case Left(failure) => Json.fromString(s"$failure")
    }

  val idL = root.id.string

  object fileReader {
    def readFile(
        file: String
    ): ZIO[FileReader with Blocking, Throwable, Stream[Nothing, String]] =
      ZIO.accessM(_.reader.readFile(file))
  }

  object messenger {
    val getProducer: ZIO[Messaging, Throwable, KafkaProducer[String, String]] =
      ZIO.accessM(_.messenger.getProducer)

    def send(
        producer: KafkaProducer[String, String],
        key: String,
        data: String
    ): ZIO[Messaging, Throwable, RecordMetadata] =
      ZIO.accessM(_.messenger.send(producer, key, data))
  }

  val testRuntime = Runtime(
    new FileReaderLive with Blocking.Live with MessagingLive with PrometheusRegistry
        with PrometheusExporters with Console.Live,
    PlatformLive.Default
  )

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def main(args: Array[String]): Unit = {
    val rmds = for {
      prd <- messenger.getProducer
      h   <- histogram.register("simple_timer", LinearBuckets(4,6,12))
      tmr <- h.startTimer()
      r   <- registry.getCurrent()
      _   <- exporters.initializeDefaultExports(r)
      hs  <- exporters.http(r, 9090)
      cnt <- counter.register("kafka_sent_messages", Array("zenv"))
      str <- fileReader.readFile("/msgs100k.json")
      //str <- ZIO.accessM((r: FileReader with Blocking) => r.reader.readFile("/msgs100k.json"))
      rmd <- str
      .mapMParUnordered(120)(
        l => messenger.send(prd, idL.getOption(toJson(l)).getOrElse("UND"), l)
        )
        .tap(md => cnt.inc(Array("zenv")) *> h.observeDuration(tmr) *> putStrLn(md.toString()))
        .runDrain
    } yield ()

    val t0 = System.currentTimeMillis()
    testRuntime.unsafeRun(rmds)
    val t1 = System.currentTimeMillis()
    println(s"Completed in ${t1 - t0} ms")
  }
}
