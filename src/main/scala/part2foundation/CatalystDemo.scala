package part2foundation

import org.apache.spark.sql.{DataFrame, SparkSession}

object CatalystDemo {

  val spark = SparkSession.builder()
    .appName("Catalyst Demo")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val flights = spark.read
    .option("inerSchema", "true")
    .json("src/main/resources/data/flights")

  val notFromHere = flights
    .where($"origin" =!= "LGA")
    .where($"origin" =!= "ORD")
    .where($"origin" =!= "SFQ")
    .where($"origin" =!= "DEN")
    .where($"origin" =!= "BOS")
    .where($"origin" =!= "EWR")

  notFromHere.explain(true)

  /*

  == Parsed Logical Plan ==
  'Filter NOT ('origin = EWR)
  +- Filter NOT (origin#18 = BOS)
     +- Filter NOT (origin#18 = DEN)
        +- Filter NOT (origin#18 = SFQ)
           +- Filter NOT (origin#18 = ORD)
              +- Filter NOT (origin#18 = LGA)
                 +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Analyzed Logical Plan ==
  _id: string, arrdelay: double, carrier: string, crsarrtime: bigint, crsdephour: bigint, crsdeptime: bigint, crselapsedtime: double, depdelay: double, dest: string, dist: double, dofW: bigint, origin: string
  Filter NOT (origin#18 = EWR)
  +- Filter NOT (origin#18 = BOS)
     +- Filter NOT (origin#18 = DEN)
        +- Filter NOT (origin#18 = SFQ)
           +- Filter NOT (origin#18 = ORD)
              +- Filter NOT (origin#18 = LGA)
                 +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json


  == Optimized Logical Plan ==
  Filter ((((((isnotnull(origin#18) AND NOT (origin#18 = LGA)) AND NOT (origin#18 = ORD)) AND NOT (origin#18 = SFQ)) AND NOT (origin#18 = DEN)) AND NOT (origin#18 = BOS)) AND NOT (origin#18 = EWR))
  +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json
  TODO -> now Catalyst has come into picture and put all filter into single line instead of signle line each filter as above in logical plan
   */


  def filterTeam1(flights: DataFrame) = flights.where($"origin" =!= "LGA").where($"dest" === "DEN")

  def filterTeam2(flights: DataFrame) = flights.where($"origin" =!= "EWR").where($"dest" === "DEN")

  val filterBoth = filterTeam1(filterTeam2(flights))

  filterBoth.explain(true)

  /*
  == Parsed Logical Plan ==
  'Filter ('dest = DEN)
  +- Filter NOT (origin#18 = LGA)
     +- Filter (dest#15 = DEN)
        +- Filter NOT (origin#18 = EWR)
           +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Analyzed Logical Plan ==
  _id: string, arrdelay: double, carrier: string, crsarrtime: bigint, crsdephour: bigint, crsdeptime: bigint, crselapsedtime: double, depdelay: double, dest: string, dist: double, dofW: bigint, origin: string
  Filter (dest#15 = DEN)
  +- Filter NOT (origin#18 = LGA)
     +- Filter (dest#15 = DEN)
        +- Filter NOT (origin#18 = EWR)
           +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Optimized Logical Plan ==
  Filter ((((isnotnull(dest#15) AND isnotnull(origin#18)) AND NOT (origin#18 = EWR)) AND (dest#15 = DEN)) AND NOT (origin#18 = LGA))
  +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json
    TODO -> in logical plan we see filters like chain filter and in this plan only single filter and also duplicate filter ignore by Catalyst


  == Physical Plan ==
  *(1) Project [_id#7, arrdelay#8, carrier#9, crsarrtime#10L, crsdephour#11L, crsdeptime#12L, crselapsedtime#13, depdelay#14, dest#15, dist#16, dofW#17L, origin#18]
  +- *(1) Filter ((((isnotnull(dest#15) AND isnotnull(origin#18)) AND NOT (origin#18 = EWR)) AND (dest#15 = DEN)) AND NOT (origin#18 = LGA))
     +- FileScan json [_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] Batched: false, DataFilters: [isnotnull(dest#15), isnotnull(origin#18), NOT (origin#18 = EWR), (dest#15 = DEN), NOT (origin#18..., Format: JSON, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-performance-tuning-master/src/main/resources/da..., PartitionFilters: [], PushedFilters: [IsNotNull(dest), IsNotNull(origin), Not(EqualTo(origin,EWR)), EqualTo(dest,DEN), Not(EqualTo(ori..., ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...

   */

  // pushing down filters
  flights.write.save("src/main/resources/data/flights_parquet")

  val notFromLGA = spark.read.load("src/main/resources/data/flights_parquet")
    .where($"origin" =!= "LGA")

  notFromLGA.explain()
  /*
  == Physical Plan ==
  *(1) Project [_id#55, arrdelay#56, carrier#57, crsarrtime#58L, crsdephour#59L, crsdeptime#60L, crselapsedtime#61, depdelay#62, dest#63, dist#64, dofW#65L, origin#66]
  +- *(1) Filter (isnotnull(origin#66) AND NOT (origin#66 = LGA))
     +- *(1) ColumnarToRow
        +- FileScan parquet [_id#55,arrdelay#56,carrier#57,crsarrtime#58L,crsdephour#59L,crsdeptime#60L,crselapsedtime#61,depdelay#62,dest#63,dist#64,dofW#65L,origin#66] Batched: true, DataFilters: [isnotnull(origin#66), NOT (origin#66 = LGA)], Format: Parquet, Location: InMemoryFileIndex[file:/D:/Data Engineering/spark-performance-tuning-master/src/main/resources/da..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), Not(EqualTo(origin,LGA))], ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
      TODO -> PushedFilters: [IsNotNull(origin), Not(EqualTo(origin,LGA))] --> whenever we see pushed filter that means catalyst include filter down to the data source( file it self not be part of Dataframe) in loading time filter will apply help of catalyst
   */

  def main(args: Array[String]): Unit = {


  }
}

// Catalyst

// Results
//  -  extra structure(SQL) limits what can be expressed(vs RDDs) means SQl can't do everything so RDDs come into picture
//  -  however, we can express most computations
//  -  expressions are more concise
//  -  structure allows for optimizations

// Spark SQL and Spark DFs are too optimized instead of RDDs (even well design RDDs implementations)

// What Catalyst can't do
//  - can't optimize "lambdas"















