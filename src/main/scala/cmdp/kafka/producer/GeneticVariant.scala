package cmdp.kafka.producer

case class GeneticVariant(termiteId: String, snpId: Long, nucleotideChange: List[String],
                                  geneInfo: List[String], variantType: String, clinicalSignificance: List[String], molecularConsequence: List[String])
