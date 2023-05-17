package part3caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Caching {

  val spark = SparkSession.builder()
    .appName("Caching")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", 10000000)
    .master("local")
    .getOrCreate()

  val flightsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights")

  flightsDF.count()

  // simulate an "expensive" operation
  val orderedFlightsDF = flightsDF.orderBy("dist")

  // scenario: use this DF multiple times
  orderedFlightsDF.persist(
    // you can use persist for RDDs and DFs as well.
    // no argument = MEMORY_AND_DISK
    //StorageLevel.MEMORY_ONLY // cache the DF in memory EXACTLY - CPU efficient, memory expensive
    //StorageLevel.DISK_ONLY // cache the DF to DISK - CPU efficient and memory efficient, but slower
    //StorageLevel.MEMORY_AND_DISK // cache this DF to both the heap AND the disk - first caches to memory, but if the DF is EVICTED, will be written to disk if storage capacity will full when executor will store the RDD,DF,DS

    /* modifiers */
    //StorageLevel.MEMORY_ONLY_SER // memory only, serialized - more CPU intensive, memory saving - more impactful for RDDs
    //StorageLevel.MEMORY_ONLY_2 // memory only, replicated twice - for resiliency(if one executor failed than other executor will do there task), 2x memory usage
    //StorageLevel.MEMORY_ONLY_SER_2 // memory only, serialized, replicated 2x

    /* off-heap */
    StorageLevel.OFF_HEAP // cache outside the JVM, done with Tungsten, still stored on the machine RAM, needs to be configured, CPU efficient and memory efficient
  )


  orderedFlightsDF.count()
  orderedFlightsDF.count()

  orderedFlightsDF.unpersist() // remove this DF from cache

  // change cache name
  orderedFlightsDF.createOrReplaceTempView("orderedFlights")
  spark.catalog.cacheTable("orderedFlights")
  orderedFlightsDF.count()

  // RDDs
  val flightsRDD = orderedFlightsDF.rdd
  flightsRDD.persist(StorageLevel.MEMORY_ONLY) // it will use deserialized one and not compress the memory/file size
  flightsRDD.persist(StorageLevel.MEMORY_ONLY_SER) // it will use serialized one and compress the memory/file size
  flightsRDD.count()

  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}

// Caching recap

// Memory-only efficient (flightsRDD.persist(StorageLevel.MEMORY_ONLY))
// - very CPU efficient
// - can increase the risk of memory failures

// Disk storage (flightsRDD.persist(StorageLevel.DISK_ONLY))
// - memory efficient
// - slow to access

// Serialization (flightsRDD.persist(StorageLevel.MEMORY_ONLY_SER))
// - more CPU intensive
// - 3x - 5x memory saving

// Replication (flightsRDD.persist(StorageLevel.MEMORY_ONLY_2))
// - 2x memory/disk usage
// - fault tolerance

// Off-heap ((flightsRDD.persist(StorageLevel.OFF_HEAP))
//  spark.memory.offHeap.enabled = true
//  spark.memory.offHeap.size = 10485760
// - free executor memory
// - needs to be configured

// Caching Tradeoffs
// Raw objects
// - consume 3x-5x more memory (either RAM or DISK)
// - take 20x less time to process in RAM
// - take more time to read from disk

// Serialized objects
// - max memory efficiency
// - CPU intensive
// - take less time to read from disk

// Fault tolerance
// - failed nodes will lose cached partitions
// - cached partitions will be recomputed by other nodes (unless replicated)

// Caching Recommendations
// Only cache what's being reused a lot
// - don't cache too much or you risk OOMing the executor
// - the LRU data will be evicted

// if data fits in memory, use MEMORY_ONLY (default)
// - most CPU efficient

// if data is larger, use MEMORY_ONLY_SER
// - more CPU intensive, but still faster than anything else

// Use disk caching only for really expensive computations(filter,projection)
// - simple filters take just as much (or even less) to recompute than reread from disk

