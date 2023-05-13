package part1recap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRecap {

  // the entry point to the Spark structured API
  val spark = SparkSession.builder()
    .appName("Spark Recap")
    .master("local[2]")
    .getOrCreate()

  // read a DF
  val cars = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars")

  import spark.implicits._

  // select
  val usefulCarsData = cars.select(
    col("Name"), // column object
    $"Year" // another column object (needs spark implicits)
      (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  val carsWeights = cars.selectExpr("Weight_in_lbs / 2.2")

  // filtering
  val europeanCars = cars.filter(col("Origin") =!= "USA")
  //val europeanCars = cars.where(col("Origin") =!= "USA")

  // aggregations
  val averageHP = cars.select(avg(col("Horsepower")).as("horsepower_avg")) // sum, mean, stddev, min, max

  // grouping
  val countByOrigin = cars
    .groupBy(col("Origin")) // a RelationalGroupedDataset
    .count()

  // joining
  val guitarPlayers = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val guitaristsBands = guitarPlayers.join(bands, guitarPlayers.col("bands") === bands.col("id"), "inner")

  /*
      Join types
      - inner : only the matching rows are kept
      - left/right/full outer join
      - semi/anti join
   */

  // datasets = typed distributed collection of objects
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  val guitarPlayerDS = guitarPlayers.as[GuitarPlayer] // need spark.implicits
  guitarPlayerDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  // low-level API : RDDs
  val sc = spark.sparkContext

  val numbersRDD: RDD[Int] = sc.parallelize(1 to 1000000)

  // functional operators
  val doubles = numbersRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("number") // yuo lose type info, you get SQL capability

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // also have Type[] info and SQL capability

  // DS -> RDD
  val guitarPlayersRDD = guitarPlayerDS.rdd

  // DF -> RDD
  val carsRDD = cars.rdd // RDD[Row]

  def main(args: Array[String]): Unit = {
    // showing a DF to the console
    cars.show()
    cars.printSchema()

  }
}


















