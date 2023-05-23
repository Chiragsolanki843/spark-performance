package part5boost

import common._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object FixingDataSkews {

  // Uneven distributed of data is called Data Skews

  val spark = SparkSession.builder()
    .appName("Fixing Data Skews")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  import spark.implicits._

  val guitars: Dataset[Guitar] = Seq.fill(40000)(DataGenerator.randomGuitar()).toDS

  val guitarSales: Dataset[GuitarSale] = Seq.fill(20000)(DataGenerator.randomGuitarSale()).toDS

  /*
      A Guitar is similar to a GuitarSale if
      - same make and model
      - abs(guitar.soundScore = guitarSale.soundScore) < 0.1

      Problem:
      - for every Guitar, avg(sale prices of ALL SIMILAR GuitarSales)
      - Gibson L-00, config "asdsadasd" sound 4.3,
        compute avg(sale prices of ALL GuitarSales of Gibson L-00 with sound quality between 4.2 and 4.4)
   */

  def naiveSolution() = {
    val joined = guitars.join(guitarSales, Seq("make", "model"))
      .where(abs(guitarSales("soundScore") - guitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    joined.explain()
    joined.count()
  }

  def noSkewSolution() = {
    // salting interval 0-99
    val explodedGuitars = guitars.withColumn("salt", explode(lit((0 to 99).toArray))) // multiplying the guitars DS x100
    explodedGuitars.show(120)
    val saltedGuitarSales = guitarSales.withColumn("salt", monotonically_increasing_id() % 100)

    val nonSkewedJoin = explodedGuitars.join(saltedGuitarSales, Seq("make", "model", "salt"))
      .where(abs(saltedGuitarSales("soundScore") - explodedGuitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    nonSkewedJoin.explain()
    nonSkewedJoin.count()

  }

  def main(args: Array[String]): Unit = {
    //naiveSolution()
    noSkewSolution()
    Thread.sleep(1000000)
  }
}

// Fixing Data Skews

// Non-uniform data distribution => non-uniform task distribution
// - straggling tasks : will delay the completion of a stage
// - not solvable with more resources

// Salting

// - explode the smaller RDD or DF with salt values for every row
// - in the other RDD/DF, add a random value from the salt interval for every row
// - join on the combined key = original key + salt column
// - combine partial results if you need to

// How salting works
// - data is now distributed by n+1 keys, one of which is uniform
// - the larger the salt interval, the less skewed tasks
// - the larger the salt interval, the larger the shuffle size (take smaller salt interval like 0 to 5 or 10) it will be help to less shuffle data

