package part3caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Checkpointing {

  val spark = SparkSession.builder()
    .appName("Checkpointing")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  def demoCheckpoint() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    // do some expensive computations
    val orderedFlights = flightsDF.orderBy("dist")

    // checkpointing is used to avoid failure in computations
    // needs to be configured
    sc.setCheckpointDir("spark-warehouse")

    // checkpoint a DF = save the DF to disk
    val checkpointedFlights = orderedFlights.checkpoint() // an action // checkpoint with arguments but no arguments then 'eager = true'  also we have to perform some action on it like count,take,collect,show etc.

    // query plan difference with checkpointed DFs
    /*
    == Physical Plan ==
    *(1) Sort [dist#16 ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(dist#16 ASC NULLS FIRST, 200), true, [id=#10]
       +- FileScan json [_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-performance-tuning-master/src/main/resources/da..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...

     */
    orderedFlights.explain()

    /*
    == Physical Plan ==
    *(1) Scan ExistingRDD[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18]
     */
    checkpointedFlights.explain()

    checkpointedFlights.show()
  }

  def cachingJobRDD() = {
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_).persist(StorageLevel.DISK_ONLY)
    descNumbers.sum()
    descNumbers.sum() // shorter time here
  }

  def checkpointingJobRDD() = {
    sc.setCheckpointDir("spark-warehouse")
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_)
    descNumbers.checkpoint() // returns Unit
    descNumbers.sum()
    descNumbers.sum()

  }

  def cachingJobDF() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    orderedFlights.persist(StorageLevel.DISK_ONLY)
    orderedFlights.count()
    orderedFlights.count() // shorter job
  }

  def checkpointingJobDF() = {
    sc.setCheckpointDir("spark-warehouse")
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    val checkpointedFlights = orderedFlights.checkpoint()
    checkpointedFlights.count()
    checkpointedFlights.count()
  }

  def main(args: Array[String]): Unit = {
    //demoCheckpoint()
    //cachingJobRDD()
    //checkpointingJobRDD()
    cachingJobDF()
    checkpointingJobDF()
    Thread.sleep(1000000)


  }
}


// Checkpointing vs Caching

// - Checkpointing has No main memory used, only disk
// - Checkpointing Takes more space and is slower than caching
// - Dependency graph is erased unlike caching
// - Disk location us usually a cluster-available file system e.g HDFS
// - Node failure with caching => partition is lost & needs to be recomputed from scratch
// - Node failure with checkpointing => partition is reloaded on another executor from HDFS.

// caching is faster than checkpointing but only one thing checkpointing give instead of caching
// if node/executor will failed to load the data in caching then our whole operation/computation will need to restart.
// and in checkpointing no need to restart our computation other node/executor will take responsibilities and complete then job.

// Checkpointing

// - Saved the RDD/DF to external storage(HDFS) and forgets its lineage
// - Makes an intermediate RDD/DF available to other jobs
// - Takes more space and is slower than caching
// - Does not use Spark memory
// - Does not force re-computation of a partitions if a node fails

// When to use What

// Use checkpointing if you can't afford a re-computation
// - example : a huge incremental dataset

// if a job is slow,use caching (it will read from memory so that performance very fast)

// if a job is failing, use checkpointing
// - OOMs are reduced as checkpoints don't use executor memory
// - network/other errors are mitigated by breaking the job into several segments

// sc.setCheckpointDir("spark-warehouse") <-- config external location to store checkpoints
// mySuperExpensiveRDD.checkpoint()