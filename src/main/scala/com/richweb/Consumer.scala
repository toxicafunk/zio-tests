package com.richweb

import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.KafkaConsumer.Conf

import org.apache.kafka.clients.consumer.{ConsumerRecords, ConsumerRecord, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

import zio.{DefaultRuntime, Task, UIO, ZIO}
import zio.console.putStrLn

import scala.collection.JavaConverters._

import io.circe.Json
import io.circe.optics.JsonPath.root
import io.circe.parser.parse

object Consumer {

  val consumer = KafkaConsumer[String, String](
    Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = "172.18.0.3:9092,172.18.0.4:9092,172.18.0.5:9092",
      groupId = "consumer08",
      enableAutoCommit = true,
      autoOffsetReset = OffsetResetStrategy.EARLIEST
    )
  )

  val subscribe = Task.effect(consumer.subscribe(List("julio.genio.stream").asJava))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  val poll: Task[Iterable[ConsumerRecord[String,String]]] = for {
    records <- Task.effect(consumer.poll(1000))
    record <- Task.succeed(records.asScala)
  } yield record

  val toJson: String => Json = txt =>
    parse(txt) match {
      case Right(j)      => j
      case Left(failure) => Json.fromString(s"$failure")
    }

  val definitionIdL = root.sessions.each.events.each.definitionId.string

  val agg: Iterable[ConsumerRecord[String,String]] => Task[Map[String, Int]] = records =>
  Task.succeed(records.map(cr => toJson(cr.value())).groupBy(definitionIdL.headOption(_).getOrElse("UND")).mapValues(_.size))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  val program = for {
    _       <- subscribe
    records <- poll
    grouped <- agg(records)
    printer <- ZIO.sequence(grouped.map(mss => putStrLn(s"${mss._1} -> ${mss._2}")))
  } yield printer


  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def main(args: Array[String]): Unit = {
    val runtime = new DefaultRuntime {}
    println("Starting!")
    val t0 = System.currentTimeMillis()
    runtime.unsafeRun(program.forever.unit)//.map((x: ConsumerRecord[String, String]) => println(x.value))
    val t1 = System.currentTimeMillis()
    println(s"Elapsed time: ${t1 - t0} ms")
  }
}
