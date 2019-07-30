package com.richweb

import scala.io.{Codec, Source}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse

import cakesolutions.kafka.KafkaProducer.Conf
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer

object Main {

  val readFile: String => List[String] = f =>
      Source
        .fromInputStream(
          this.getClass.getResourceAsStream(f)
        )
        .getLines()
        .toList

  val toJson: String => Json = txt =>
    parse(txt) match {
      case Right(j)      => j
      case Left(failure) => Json.fromString(s"$failure")
    }

  val idL = root.id.string

  val getProducer =
    KafkaProducer(
      Conf(
        new StringSerializer(),
        new StringSerializer(),
        bootstrapServers = "172.18.0.3:9092,172.18.0.4:9092,172.18.0.5:9092"
      )
    )

  val sendMessage
      : (KafkaProducer[String, String], String) => Future[RecordMetadata] =
    (producer, data) => {
        val record = KafkaProducerRecord(
          "julio.genio.stream",
          idL.getOption(toJson(data)).getOrElse(""),
          data
        )
        producer.send(record)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def main(args: Array[String]): Unit = {
    val prd = getProducer
    val futs = for {
      line <- readFile("/msgs100k.json")
    } yield sendMessage(prd, line)

    futs.foreach(fut => {
      val rmd = Await.result(fut, 2.seconds)
      println(rmd.toString())
    })
  }
}
