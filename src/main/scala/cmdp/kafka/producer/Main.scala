package cmdp.kafka.producer

object Main extends App {

// val producer = new Fs2KafkaProducerDebug(5, 20)
//  producer.produceRecords("/home/ganevae/Downloads/part-00000-tid-6181780790351662560-034ab762-2ef7-4aa1-90fb-5bcd6949625b-2356-1-c000.csv",
//    getProducerPropertiesAsMap("test-topic"))

  val producer = new Fs2DbfsKafkaProducer(5, getProducerPropertiesAsMap("test-topic"))
  producer.produceRecords("https://bce-genetic-variants-bucket.s3.eu-west-1.amazonaws.com/input/20.tsv")

  private def getProducerPropertiesAsMap(topic: String): Map[String, String] = {

    Map(
      "bootstrap.servers" -> "localhost:29092",
      "topic" -> topic,
      "batch.size" -> "16777216", //16MB
      "linger.ms" -> "10000",
      "retries" -> Int.MaxValue.toString,
      "retry.backoff.ms" -> "100",
      "retry.backoff.max.ms" -> "1000"
    )
  }
}
