package com.richweb

import scala.io.{Codec, Source}
import scala.concurrent.Future

import zio.{App, Task, UIO, ZIO}
import zio.console.putStrLn
import zio.stream._

import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse

import cakesolutions.kafka.KafkaProducer.Conf
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer

object Main extends App {

  val readFile: String => Task[List[String]] = f =>
    Task.effect(
      Source
        .fromInputStream(
          this.getClass.getResourceAsStream(f)
        )
        .getLines()
        .toList
    )

  val toJson: String => Json = txt =>
    parse(txt) match {
      case Right(j)      => j
      case Left(failure) => Json.fromString(s"$failure")
    }

  val idL = root.id.string

  val producer = Task.effect(
    KafkaProducer(
      Conf(
        new StringSerializer(),
        new StringSerializer(),
        bootstrapServers = "172.18.0.2:9092,172.18.0.4:9092,172.18.0.5:9092"
      )
    )
  )

  val sendMessage
      : (KafkaProducer[String, String], String) => Task[RecordMetadata] =
    (producer, data) =>
      ZIO.fromFuture(implicit ctx => {
        val record = KafkaProducerRecord(
          "julio.genio.stream",
          idL.getOption(toJson(data)).getOrElse(""),
          data
        )
        producer.send(record)
      })

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def run(args: List[String]): ZIO[Environment, Nothing, Int] = {
    val rmds = for {
      prd <- producer
      lst <- readFile("/msgs100k.json")
      rmd <- Task.foreach(lst)(l => sendMessage(prd, l))
      _   <- ZIO.foreach_(rmd)(md => putStrLn(md.toString()))
    } yield ()
    rmds.foldM(_ => UIO(1), _ => UIO(0))
  }
}
