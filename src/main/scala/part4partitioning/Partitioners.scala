package part4partitioning

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Partitioners {

  val spark = SparkSession.builder()
    .appName("Partitioners")
    .master("local") // we need single thread running that job. if we took multiple thread then result will different
    // byLengthZWords.foreachPartition(_.foreach(println)) this line print by multiple thread if we use local 1 or *
    // if we use multiple cores then result will print multiple on same time.
    .getOrCreate()

  val sc = spark.sparkContext

  //  val numbers = sc.parallelize(1 to 10000)
  //
  //  println(numbers.partitioner) // get NONE // spark has no info for partitioner
  //
  //  val numbers3 = numbers.repartition(3) // random data redistribution
  //
  //  println(numbers3.partitioner) // get NONE // spark still has no info for partitioner
  //
  //  // keep track of the partitioner
  //  // KV RDDs can control the partitioning scheme
  //
  //  val keyedNumbers = numbers.map(n => (n % 10, n)) // RDD[(Int, Int)]
  //
  //  val hashedNumbers = keyedNumbers.partitionBy(new HashPartitioner(4))
  //  /*
  //       keys with the same hash stay on the same partition
  //       Prerequisite for
  //        - combineByKey
  //        - groupByKey
  //        - aggregateByKey
  //        - foldByKey
  //        - reduceByKey
  //        Prerequisite for joins, when neither RDD has a known partitioner.
  //  */
  //  val rangedNumbers = keyedNumbers.partitionBy(new RangePartitioner(5, keyedNumbers))
  //
  //  /*
  //      Keys within the same range will be on the same partitioner.
  //      For a spectrum 0-1000
  //      keys between INt.MinValue-200 => partition    0
  //      keys between 200-400          => partition    1
  //      keys between 400-600          => partition    2
  //      keys between 600-800          => partition    3
  //      keys between 800-Int.MaxValue => partition    4
  //
  //      RangePartitioner is a prerequisite for a SORT.
  //   */
  //
  //  rangedNumbers.sortByKey() // NOT incur a shuffle // already data will sorted so no need to sort again.

  // define your own partitioner

  def generateRandomWords(nWords: Int, maxLength: Int) = {
    val r = new Random()
    (1 to nWords).map(_ => r.alphanumeric.take(r.nextInt(maxLength)).mkString(""))
  }

  val randomWordsRDD = sc.parallelize(generateRandomWords(1000, 100))

  // repartition this RDD by the Words length == two words of the same length will be on the same partition
  // customer computation = counting the occurrences of 'z' in every word

  val zWordsRDD = randomWordsRDD.map(word => (word, word.count(_ == 'z'))) // RDD[(String, Int)]

  class ByLengthPartitioner(override val numPartitions: Int) extends Partitioner {

    // override def numPartitions: Int = 100 // we can define constructor arguments

    override def getPartition(key: Any): Int = {
      key.toString.length % numPartitions
    }
  }

  val byLengthZWords = zWordsRDD.partitionBy(new ByLengthPartitioner(100))


  def main(args: Array[String]): Unit = {

    byLengthZWords.foreachPartition(_.foreach(println)) // '_' is iterator of records and going to print it out.

  }
}

// Partitioners

// Decide which record stays on which partition (key-value RDDs only, not for DF)
// - hash partitioning = same hashCode, same partition
// - range partitioning = same range, same partition
// - custom partitioning = you decide where each keys stays, for custom computations

// Partitioning has advantages and does not incur shuffles
// - sort/orderBy => RangePartitioning
// - aggregation by key => HashPartitioning
// - join => both DFs obey HashPartitioning
// - repartition with a number => RoundRobinPartitioning
// - repartition by column => HashPartitioning

// Joins Speedup

// Make sure the same keys are on the same partition
// - RDDs must have the same partitioner
// - otherwise, Spark will pick one

// Co-partitioning : RDDs share the same partitioner
// - no shuffle involved for joins

// Co-location : RDD partitions are already loaded in memory
// - fastest join possible

