package part2foundation

import org.apache.spark.sql.SparkSession

object TungstenDemo {

  val spark = SparkSession.builder()
    .appName("Tungsten Demo")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  val numbersRDD = sc.parallelize(1 to 10000000).cache() // cache will save RDD in memory so we can use later
  numbersRDD.count()
  numbersRDD.count() // much faster (not evaluated any more but it will fetch result from memory)

  import spark.implicits._

  val numbersDF = numbersRDD.toDF("value").cache() // cached with 'Tungsten'

  numbersDF.count()
  numbersDF.count() // much faster (faster then previous job(RDD))

  // TODO ->Tungsten is active in 'WholeStageCodegen' // when we see 'wholeStageCodegen' in DAG at spark UI  means 'Tungsten' working in behind

  /*
  == Physical Plan ==
  HashAggregate(keys=[], functions=[sum(id#54L)])
  +- HashAggregate(keys=[], functions=[partial_sum(id#54L)])
     +- Range (0, 1000000, step=1, splits=1)
   */
  spark.conf.set("spark.sql.codegen.wholeStage", "false") // Deactivated wholeStageCodegen and its boolean configuration.

  val noWholeStageSum = spark.range(1000000).selectExpr("sum(id)") // 'id' is name of the column which spark.range will created

  noWholeStageSum.explain()
  noWholeStageSum.show()

  /*
  == Physical Plan ==
  *(1) HashAggregate(keys=[], functions=[sum(id#66L)])
  +- *(1) HashAggregate(keys=[], functions=[partial_sum(id#66L)])
     +- *(1) Range (0, 1000000, step=1, splits=1)

     '*' means that Tungsten is present!
   */

  spark.conf.set("spark.sql.codegen.wholeStage", "true")

  val wholeStageSum = spark.range(1000000).selectExpr("sum(id)")

  wholeStageSum.explain()
  wholeStageSum.show()


  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}

// Tungsten

// How we see it
// - in the query plans
// - tasks called "SerializeFromObject" - serialize the data from the beginning
// - tasks with asterisk(*) use Tungsten

// Benefits
// - faster serialization than Kryo and >>>>> Java
// - much smaller object size than Java
// - supports of-heap allocation
// - supports Spark operations without serialization --> e.g you can sort the data while in binary
// - avoids the JVM's GC
// - much faster, less memory, less CPU
// - can process much larger datasets

// How do we enable it (it's enabled and free!)? -->TODO spark.sql.tungsten.enabled = true
