package com.richweb

import scala.io.{Codec, Source}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse

import cakesolutions.kafka.KafkaProducer.Conf
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer

import zio.{App, Task,UIO, ZIO}
import zio.console.putStrLn

object Producer extends App {

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

  val getProducer =
    Task.effect(
      KafkaProducer(
        Conf(
          new StringSerializer(),
          new StringSerializer(),
          bootstrapServers = "172.18.0.3:9092,172.18.0.4:9092,172.18.0.5:9092"
        )
      )
    )

  val sendMessage
      : (KafkaProducer[String, String], String, String) => Task[RecordMetadata] =
    (producer, key, body) => {
      val record = KafkaProducerRecord(
        "julio.genio.stream",
        key,
        body
      )
      Task.fromFuture(implicit ctx => producer.send(record))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def run(args: List[String]): ZIO[Environment, Nothing, Int] = {
    val program = for {
    producer <- getProducer
    msgs     <- readFile("/msgs100k.json")
    futs     <- Task.collectAll(msgs.map(l => {
                   val id = idL.getOption(toJson(l)).getOrElse("UND")
                   sendMessage(producer, id, l)
                  }))
     prnt      <- ZIO.foreach(futs)(rmd => putStrLn(rmd.toString()))
    } yield prnt

    program.foldM(_ => UIO(1), _ => UIO(0))
  }
}
