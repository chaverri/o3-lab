import java.io.FileWriter
import java.text.SimpleDateFormat

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object CsvAnalyzer {

  //I found this regex to evaluate the geo type of data, but the criteria specified was: 9999 or 99999-9999
  /*val latitudeRegexPattern =  "^(\\+|-)?(?:90(?:(?:\\.0{1,6})?)|(?:[0-9]|[1-8][0-9])(?:(?:\\.[0-9]{1,6})?))$"
  val longitudeRegexPattern = "^(\\+|-)?(?:180(?:(?:\\.0{1,6})?)|(?:[0-9]|[1-9][0-9]|1[0-7][0-9])(?:(?:\\.[0-9]{1,6})?))$"*/

  val latitudeRegexPattern = "^[0-9]{4}$"
  val longitudeRegexPattern = "^[0-9]{5}-[0-9]{4}$"
  val BINARY_CARDINALITY = 2;

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)


  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(CsvAnalyzer.getClass)

    val conf = new SparkConf().setAppName("CSV Analyzer")
    val sc = new SparkContext(conf)

    require(args.length == 2, "The filename and date format should be provided")

    val fileName = args(0)

    require(fileName != null && fileName.length > 0, "The filename can not be empty")

    val dateFormat = args(1)

    require(dateFormat != null && dateFormat.length > 0, "The dateFormat can not be empty")

    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat(dateFormat)
    simpleDateFormat.setLenient(true);

    logger.info("Using file: " + fileName + " dateFormat: " + dateFormat)

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
        observation.zipWithIndex.map { case (observationValue, index) => (index, observationValue) }
      }.groupBy { case (index, value) => index }

    def determineType(value: String, cardinality: Integer): DataTypes = {

      if (cardinality == BINARY_CARDINALITY)
        return Binary

      if (value.matches(latitudeRegexPattern) || value.matches(longitudeRegexPattern)) {
        return Geo
      }

      Try(value.toDouble) match {
        case Success(i) => return Numeric
        case Failure(i) =>
      }

      Try(simpleDateFormat.parse(value)) match {
        case Success(i) => return Date
        case Failure(i) =>
      }

      return Categorical;

    }

    val valuesByFeature = headerRDD.join(observationsByFeature)
                          .map{case (index,(featureName,values)) =>  (featureName, values.map{case (featureIndex, value) => value}.par)} //simplify the data structure
                          .cache()

    val results = valuesByFeature.map { case (featureName, _observationsByFeature) =>

      val observations = _observationsByFeature.size

      val cardinality = _observationsByFeature.toSet.size;

      val foundTypes = _observationsByFeature.map(value => determineType(value, cardinality).name).toSet

      (featureName, observations, cardinality, foundTypes)

    }.collect()

    val fileWriter = new FileWriter(fileName + ".statistics.json")

    /*results.foreach { case (featureName, observationCount, cardiniality, foundtTypes) =>
      println("--------------------------------------")
      println("Feature name: " + featureName)
      println("Cardinality: " + cardiniality)
      println("Observations: " + observationCount)
      println("Types: " + foundtTypes)
      println("--------------------------------------")
      println()
    }*/

    mapper.writeValue(fileWriter, results.map { case (featureName, observationCount, cardiniality, foundtTypes) =>
      new FeatureInfo(name = featureName,
        cardinality = cardiniality,
        observations = observationCount,
        dataTypes = foundtTypes.seq)
    })

    fileWriter.close()

  }


  trait DataTypes { def name: String }

  case object Binary extends DataTypes { val name = "Binary" }

  case object Numeric extends DataTypes { val name = "Numeric" }

  case object Geo extends DataTypes { val name = "Geo" }

  case object Date extends DataTypes { val name = "Date" }

  case object Categorical extends DataTypes { val name = "Categorical" }

  case class FeatureInfo(name: String, cardinality: Integer, observations: Integer, dataTypes: Set[String])

}
