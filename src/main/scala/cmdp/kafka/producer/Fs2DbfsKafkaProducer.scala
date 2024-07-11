package cmdp.kafka.producer

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.readInputStream
import fs2.kafka._
import fs2.text
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import upickle.legacy
import upickle.legacy.{ReadWriter, macroRW}

import java.io.InputStream

class Fs2DbfsKafkaProducer(chunkSize: Int, producerProperties: Map[String, String]) {

  implicit val ownerRw: ReadWriter[GeneticVariant] = macroRW

  private def streamFromS3(filePath: String): InputStream = {
    val path = java.nio.file.Paths.get(filePath.replace("dbfs:/", "/dbfs/"))

    java.nio.file.Files.newInputStream(path)
  }

  def produceRecords(filePath: String): Unit = {
    val producerProps: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String].withProperties(producerProperties)

    val producerSettings = TransactionalProducerSettings(
      producerProperties("transactional.id"),
      producerProps
    )

    TransactionalKafkaProducer
      .stream(producerSettings)
      .flatMap { producer =>
        readInputStream[IO](IO(streamFromS3(filePath)), chunkSize)
          .through(text.utf8.decode)
          .through(text.lines)
          .filterNot(s => s == null || s.isEmpty)
          .chunkN(chunkSize)
          .prefetch
          .map {_.toList map toGeneticVariant}
          .map {
            _.map { geneticVariant =>
              ProducerRecord(producerProperties("topic"),
                geneticVariant.snpId.toString,
                legacy.write(geneticVariant))
            }
          }
          .map(records => ProducerRecords(records))
          .evalMap(producer.produceWithoutOffsets)
          .attempt
          .evalMap {
            case Right(_) => IO.unit
            case Left(e: ProducerFencedException) => IO.raiseError(new RuntimeException("Producer fenced, restarting required", e))
            case Left(e: InvalidProducerEpochException) => IO.raiseError(new RuntimeException("Invalid producer epoch, restarting required", e))
            case Left(e) => IO.raiseError(e)
          }
      }
      .compile.drain.unsafeRunSync()
  }

  private val toGeneticVariant = (line: String) => {
    val columns = line.split("\t", -1)
    GeneticVariant(
      columns(0),
      columns(1).toLong,
      columns(2).split(",").filter(_.nonEmpty).toList,
      columns(3).split(",").filter(_.nonEmpty).toList,
      columns(4),
      columns(5).split(",").filter(_.nonEmpty).toList,
      columns(6).split(",").filter(_.nonEmpty).toList
    )
  }
}