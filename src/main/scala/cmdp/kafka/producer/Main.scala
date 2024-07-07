package cmdp.kafka.producer

object Main extends App {

  FS2KafkaProducerDebug.produceRecords("/home/ganevae/Downloads/20.tsv",
    getProducerPropertiesAsMap("test-topic", "transactionalId-1"))


  private def getProducerPropertiesAsMap(topic: String, transactionalId: String): Map[String, String] = {

    Map(
      "bootstrap.servers" -> "localhost:29092",
      "topic" -> topic,
      "transactional.id" -> transactionalId
    )
  }
}
