import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object CsvAnalyzer {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(CsvAnalyzer.getClass)

    val conf = new SparkConf().setAppName("CSV Analyzer")
    val sc = new SparkContext(conf)

    val fileName = args(0)

    logger.info("Using file: " + fileName)

    val csv = sc.textFile(fileName)

    val observationsFile = csv.map(line => line.split(','))

    val header = observationsFile.first()

    //headerRDD of type [(0, "name0"), (1, "name1"), ...]
    val headerRDD = sc.parallelize(observationsFile.first()).map(featureName => (header.indexOf(featureName), featureName))

    //Maybe do an array/map of rdd's filtering the observations by index, so that the final operations can be performed over each rdd insead a seq value


    //[(0,[valueFeature0-observation1,valueFeature0-observation2,...]),(1,[valueFeature1-observation1,valueFeature1-observation2,...])]
    val observationsByFeature = observationsFile
      .filter(observation => observation(0) != header(0))
      .flatMap { case (observation: Array[String]) =>
        observation.zipWithIndex.map{case(observationValue, index) => (index, observationValue)}
      }.groupBy { case (index, value) => index }

    val valuesByFeature = headerRDD.join(observationsByFeature)
                          .map{case (index,(featureName,values)) =>  (featureName, values.map{case (featureIndex, value) => value}.toSeq)} //simplify the data structure
                          .cache()

    val results = valuesByFeature.map { case (featureName, observationsByFeature) =>
      (featureName, observationsByFeature.size, observationsByFeature.distinct.size)
    }.collect()


    results.foreach{ case (featureName, observationCount, cardiniality) =>
      println("--------------------------------------")
      println("Feature name: " + featureName)
      println("Cardinality: " + cardiniality)
      println("Observations: " + observationCount)
      println("--------------------------------------")
        println()
    }

    def determineType(value: String): DataTypes = {
      Categorical
    }

  }


  trait DataTypes

  case object Numeric extends DataTypes

  case object Geo extends DataTypes

  case object Date extends DataTypes

  case object Categorical extends DataTypes

  case object Binary extends DataTypes

}
