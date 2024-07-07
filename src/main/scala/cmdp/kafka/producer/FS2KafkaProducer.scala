package cmdp.kafka.producer

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.fasterxml.jackson.databind.ObjectMapper
import fs2.io.file._
import fs2.kafka._
import fs2.text

object FS2KafkaProducer {
  private val OBJECT_MAPPER = new ObjectMapper()

  private case class GeneticVariant(termiteId: String, snpId: Long, nucleotideChange: List[String],
                                    geneInfo: List[String], variantType: String, clinicalSignificance: List[String], molecularConsequence: List[String])

  def produceRecords(filePath: String, producerProperties: Map[String, String]): Unit = {
    val producerSettings = TransactionalProducerSettings(
      producerProperties("transactional.id"),
      ProducerSettings[IO, String, String]
        .withEnableIdempotence(true)
        .withRetries(10)
        .withProperties(producerProperties)
    )

    TransactionalKafkaProducer
      .stream(producerSettings)
      .flatMap { producer =>
        Files[IO].readUtf8Lines(Path(filePath))
          .through(text.lines)
          .map(toGeneticVariant)
          .chunkN(5)
          .map {
            _.toList
          }
          .map {
            _.map { geneticVariant =>
              ProducerRecord(producerProperties("topic"),
                geneticVariant.snpId.toString,
                OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(geneticVariant))
            }
          }
          .map { records => ProducerRecords(records) }
          .evalMap(records => producer.produceWithoutOffsets(records))
      }
      .compile.drain.unsafeRunSync()
  }

  private val toGeneticVariant = (line: String) => {
    val columns = line.split("\t")
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