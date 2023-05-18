package part4partitioning

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator

object PartitioningProblems {

  val spark = SparkSession.builder()
    .appName("Partitioning Problems")
    .master("local[*]") // for parallelism
    .getOrCreate()

  def processNumbers(nPartitions: Int) = {
    val numbers = spark.range(100000000) // in memory it will take ~800MB
    val repartitionedNumbers = numbers.repartition(nPartitions)
    repartitionedNumbers.cache()
    repartitionedNumbers.count() // dummy action and we don't care about it

    // the computation I care about
    repartitionedNumbers.selectExpr("sum(id)").show()
  }

  // 1 - use size estimator
  def dfSizeEstimator() = {
    val numbers = spark.range(100000)
    println(SizeEstimator.estimate(numbers)) // usually works, not super accurate, within an order of magnitude - give larger number (it will estimate the size of DS/RDD/DF)
    // measures the memory footprint of the actual object backing the Dataset
    numbers.cache()
    numbers.count()
  }

  // 2 - use query plan
  def estimateWithQueryPlan() = {
    val numbers = spark.range(100000) // store in long and long store 8 bytes of data. so result will be 800000
    println(numbers.queryExecution.optimizedPlan.stats.sizeInBytes) // accurate size in bytes for the DATA
  } // dataset or Dataframe took memory less than RDDs

  def estimateRDD() = {
    val numbers = spark.sparkContext.parallelize(1 to 100000)
    numbers.cache()
    numbers.count()
  } // RDDs will took memory larger than DS,DF

  def main(args: Array[String]): Unit = {
    //    processNumbers(2) // 400MB // partition
    //    processNumbers(20) // 40MB // partition
    //    processNumbers(200) // 4MB // partition
    //    processNumbers(2000) // 400KB // partition
    //    processNumbers(20000) // 40KB // partition
    //    dfSizeEstimator()
    //    estimateWithQueryPlan()
    //    estimateRDD()
    // 10 - 100MB rule for partition size for UNCOMPRESSED Data (compressed data for DS,DF and uncompressed data for RDDs)

    Thread.sleep(1000000000)
  }
}

// Perf = f(#partitions)
//
// Number of partitions = degree of parallelism
// - one partition = serial computation
// - multiple partitions & multiple cores = parallel/ distributed processing
//
// Optimal partition size between 10 and 100MB
// - not hard numbers - variations acceptable
// - an order of magnitude usually not acceptable
//
// Can configure the number of partitions at a shuffle
// - default : 200 for DFs, number of #cores for RDDs when launch spark shell/spark job
// set in spark.sql.shuffle.partitions for DFs
// set in spark.default.parallelism for RDDs
//
// spark.sql.shuffle.partitions = 1000
// spark.default.parallelism = 100
//
// Partitioning
//
// Optimal #partitions
// - too few partitions = not enough parallelism
// - too many partitions = thread context switch for executor
//
// Optimal partition size = 10 - 100MB of uncompressed data
//
// Determining the size of data :
// - cache size : DF "native" size (compressed), uncompressed for RDDs
// - SizeEstimator : not super accurate, but worth getting the order of magnitude
// - Query plan size in bytes : uncompressed data (DFs only)
//
// Trail and error (trail many times but some time give error)


