package com.richweb.filereader

import scala.io.{Codec, Source}

import zio.ZIO
import zio.blocking._
import zio.stream._

object FileReader {
  trait Service {
    def readFile(file: String): ZIO[Blocking, Throwable, Stream[Nothing, String]]
  }
}

trait FileReader {
  val reader: FileReader.Service
}

trait FileReaderLive extends FileReader {
  val reader: FileReader.Service = new FileReader.Service {
    override def readFile(file: String): ZIO[Blocking, Throwable, Stream[Nothing, String]] =
      for {
        ite <- blocking(ZIO.effect(Source.fromFile(file).getLines().toIterable))
        str <- ZIO.effect(Stream.fromIterable(ite))
      } yield str
  }
}

object FileReaderLive extends FileReaderLive with Blocking.Live
