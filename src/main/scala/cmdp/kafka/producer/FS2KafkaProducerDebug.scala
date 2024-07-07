package cmdp.kafka.producer

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.fasterxml.jackson.databind.ObjectMapper
import fs2.io.file._
import fs2.kafka._
import fs2.text

object FS2KafkaProducerDebug {
  private val OBJECT_MAPPER = new ObjectMapper()

  private case class GeneticVariant(termiteId: String, snpId: Long, nucleotideChange: List[String],
                                    geneInfo: List[String], variantType: String, clinicalSignificance: List[String], molecularConsequence: List[String])

  def produceRecords(filePath: String, producerProperties: Map[String, String]): Unit = {
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:29092")

    KafkaProducer
      .stream(producerSettings)
      .flatMap { producer =>
        Files[IO].readAll(Path(filePath))
          .through(text.utf8.decode)
          .through(text.lines)
          .map(toGeneticVariant)
          .chunkN(5)
          .map(_.toList)
          .evalMap { chunk =>
            val records = chunk.map { geneticVariant =>
              ProducerRecord[String, String](
                producerProperties("topic"),
                geneticVariant.snpId.toString,
                OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(geneticVariant)
              )
            }
            producer.produce(ProducerRecords(records)).flatten
          }
      }
      .compile.drain.unsafeRunSync()
  }

  private val toGeneticVariant = (line: String) => {
    val columns = line.split("\t")
    if(columns.size < 6) {
      GeneticVariant("", 0, null, null, "SNV", null, null)
    } else {
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
}