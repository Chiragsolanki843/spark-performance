package part4partitioning

import org.apache.spark.sql.SparkSession

object RepartitionCoalesce {

  val spark = SparkSession.builder()
    .appName("Repartition and Coalesce")
    .master("local[*]") // local[*] is important for parallelize the data into partition
    .getOrCreate()

  val sc = spark.sparkContext

  val numbersRDD = sc.parallelize(1 to 10000000)
  println(numbersRDD.partitions.length) // number of virtual cores

  // repartition
  val repartitionedNumbers = numbersRDD.repartition(2) // your computer 2 cores will do repartition
  repartitionedNumbers.count() // just force to evaluated for computation --> perform action on it

  // coalesce - fundamentally different
  val coalesceNumbers = numbersRDD.coalesce(2) // for a smaller number of partitions // it will reduce the partition

  coalesceNumbers.count() // just force to evaluated for computation--> perform action on it

  // 30x perf! increase when use coalesce

  // force coalesce to be a shuffle
  val forcedShuffledNumbers = numbersRDD.coalesce(2, true) // force a shuffle

  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}

// Repartition vs Coalesce
// - Repartition redistributes the data evenly across partitions (repartition is full shuffle)

// - Coalesce 'stitches' existing partitions --> coalesce haven't shuffle the data but data evenly not distributes across the partitions
//  Coalesce is very faster than Repartition

// Coalesce is a narrow dependency
// - one input(parent) partition influences a single output(child) partition

// Coalesce will still move some data
// - not a full shuffle
// - almost always faster than a shuffle

// Repartition & Coalesce

// Repartition
// - returns a new RDD with the specified number of partitions
// - will always involve a shuffle
// - prioritizes even distribution of data
// - necessary to control the number of partitions & partition size

// Coalesce
// - returns a new RDD with the specified number of partitions
// - used to decrease the number of partitions
// - in this case
//    1. coalesce is a Narrow transformation
//    2. cannot guarantee uniform data distribution
// - can also be used to increase number of partitions
//  1. essentially a repartition

// When to Use What

// Use repartition when
// - you want to increase parallelism/number of partitions
// - you want to control partition size
// - you want to redistribute the data evenly

// Use coalesce when
// - you want to reduce the number of partitions & improve perf
// - you don't care how data is distributed