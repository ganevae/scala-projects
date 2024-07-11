package cmdp.kafka.producer

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.{Files, Path}
import fs2.kafka._
import fs2.{Stream, text}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}

class Fs2KafkaProducerDebug(chunkSize: Int, batchSize: Int) {

  implicit val ownerRw: ReadWriter[GeneticVariant] = macroRW

  def produceRecords(filePath: String, settings: Map[String, String]): Unit = {

    val s3Client = S3AsyncClient.builder().build()

    def streamFromS3(bucket: String, key: String): Stream[IO, Byte] = {
      Stream.eval(IO.async_[Array[Byte]] { cb =>
        val request = GetObjectRequest.builder().bucket(bucket).key(key).build()
        val responseTransformer = AsyncResponseTransformer.toBytes[software.amazon.awssdk.services.s3.model.GetObjectResponse]()

        s3Client.getObject(request, responseTransformer).whenComplete { (response, err) =>
          if (err != null) cb(Left(err))
          else cb(Right(response.asByteArray()))
        }
      }).flatMap(Stream.emits(_)).covary[IO]
    }
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:29092")
        .withRetries(0)
        .withProperties(settings)

    KafkaProducer
      .stream(producerSettings)
      .flatMap { producer =>
        Files[IO].readAll(Path(filePath))
          .through(text.utf8.decode)
          .through(text.lines)
          .filterNot(s => s == null || s.isEmpty)
          .chunkN(chunkSize)
          .prefetch
          .map {_.toList map toGeneticVariant}
          .map {
            _.map { geneticVariant => {
                val json = write(geneticVariant)
                println(json)
                ProducerRecord("test-topic",
                  geneticVariant.snpId.toString,
                  json)
              }
            }
          }
          .map(records => ProducerRecords(records))
          .evalMap(producer.produce)
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
    val columns: Array[String] = line.split("\t", -1)

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