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

  val readFile: String => Stream[Nothing, String] = f =>
  Stream.fromIterable(
    Source
      .fromInputStream(
        this.getClass.getResourceAsStream(f)
      )
      .getLines()
      .toIterable
  )

  val toJson: String => Json = txt => parse(txt) match {
    case Right(j) => j
    case Left(failure) => Json.fromString(s"$failure")
  }

  val idL = root.id.string

  val producer = KafkaProducer(
    Conf(new StringSerializer(), new StringSerializer(), bootstrapServers = "172.18.0.3:9092,172.18.0.4:9092,172.18.0.5:9092")
  )

  val sendMessage: String => Task[RecordMetadata] = (data: String) => ZIO.fromFuture ( implicit ctx => {
    val record = KafkaProducerRecord("julio.genio.stream", idL.getOption(toJson(data)).getOrElse(""), data)
    producer.send(record)
  })

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override def run(args: List[String]): ZIO[Environment, Nothing, Int] = {
    readFile("/messages.txt")
      .tap(l => sendMessage(l) >>= (rmd => putStrLn(rmd.toString())))
      //.foreach(l => sendMessage(l) >>= (rmd => putStrLn(rmd.toString())))
      .runDrain
      .foldM(_ => UIO(1), _ => UIO(0))
  }
}