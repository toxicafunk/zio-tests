package com.richweb.messaging

import zio.{Task, ZIO}

import cakesolutions.kafka.KafkaProducer.Conf
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord}

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer

object Messaging {
  trait Service {
    def send(
        producer: KafkaProducer[String, String],
        key: String,
        data: String
    ): Task[RecordMetadata]

    def getProducer(): Task[KafkaProducer[String, String]]
  }
}

trait Messaging {
  val messenger: Messaging.Service
}

trait MessagingLive extends Messaging {

  override val messenger: Messaging.Service = new Messaging.Service {
    val getProducer = Task.effect(
      KafkaProducer(
        Conf(
          new StringSerializer(),
          new StringSerializer(),
          bootstrapServers = "172.18.0.2:9092,172.18.0.4:9092,172.18.0.5:9092"
        )
      )
    )

    override def send(producer: KafkaProducer[String, String], key: String, data: String): Task[RecordMetadata] =
        ZIO.fromFuture(implicit ctx => {
          val record = KafkaProducerRecord(
            "julio.genio.stream",
            key,
            data
          )
          producer.send(record)
        })
  }
}

object MessagingLive extends MessagingLive
