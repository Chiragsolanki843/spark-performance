package part2foundation

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TestDeployApp {

  // TestDeployApp inputFile and outFile
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Need input file and output file")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .appName("TestDeploy App")
      // method 1
      .config("spark.executor.memory", "1g")
      .getOrCreate()

    import spark.implicits._

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      $"Title",
      $"IMDB_Rating".as("Rating"),
      $"Release_Date".as("Release")
    )
      .where(($"Major_Genre" === "Comedy") and ($"IMDB_Rating" > 6.5))
      .orderBy($"Rating".desc_nulls_last)
      .repartition(1)

    // method 2
    spark.conf.set("spark.executor.memory", "1g") // warning - not all configurations available
    // its not legal bcz while spark application running
    // this will not allow for executor some setting for executor so best thing you have to define this value when spark object create.


    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))

  }
}

/** TODO COMMANDS FOR DEPLOY SPARK APPLICATION IN CLIENT MODE
  * /spark/bin/spark-submit
  * --class part2foundation.TestDeployApp
  * --master spark://38351154d8ff:7077
  * --conf spark.executor.memory 1g or --executor-memory 1g // TODO method 3
  * --deploy-mode client
  * --verbose
  * --supervise
  * /opt/spark-apps/spark-optimization-2.jar
  * /opt/spark-data/movies.json
  * /opt/spark-data/goodComedies.json
  */

/** Some command line argument to pass:
  * -- executor-memory : allocate a certain amount of RAM/executor (we'll learn later how to choose values)
  * -- driver-memory : allocate a certain amount of RAM for the driver
  * -- jars: add additional JVM libraries for Spark to have access to
  * -- packages: add additional libraries as Maven coordinates
  * -- conf (configName) (configValue) : other configurations to the Spark application (including JVM options)
  * -- help: show all options
  */
