package com.htx

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import com.htx.FinalOutput

object ItemRanking {

  def main(args: Array[String]): Unit = {
    // Step 1. Configuration Setup ---
    if (args.length != 4) {
      println("Usage: TopItemRanking <input_path_A> <input_path_B> <output_path> <top_x>")
      sys.exit(1)
    }

    val inputPathA = args(0)
    val inputPathB = args(1)
    val outputPath = args(2)
    val topX = args(3).toInt

    val spark = SparkSession.builder()
      .appName(s"ItemRanking-Top$topX")

      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", "1000")

      .getOrCreate()

    import spark.implicits._

    try {

      // Step 2. Read and Prepare RDD A (Detections) ---
      val dfA = spark.read.parquet(inputPathA)
      val rddA = dfA.select("geographical_location_oid", "detection_oid", "item_name").rdd

      // Step 3: Map to (detection_oid, (loc_oid, item_name))
      val initialMappedRdd: RDD[(Long, (Long, String))] = rddA.map { row =>
        val locOid = row.getAs[Long]("geographical_location_oid")
        val detOid = row.getAs[Long]("detection_oid")
        val itemName = row.getAs[String]("item_name")
        (detOid, (locOid, itemName))
      }

      // Step 4: Deduplicate by detection_oid (shuffle 1)
      val uniqueDetections: RDD[(Long, String)] = initialMappedRdd
        .reduceByKey((a, _) => a)
        .values // keep (loc_oid, item_name)

      // Step 5: Count (loc_oid, item_name) pairs (shuffle 2)
      val countsRdd: RDD[((Long, String), Int)] = uniqueDetections
        .map(key => (key, 1))
        .reduceByKey(_ + _)

      // Step 6: Map to (loc_oid, (item_name, count))
      val prepForJoin: RDD[(Long, (String, Int))] = countsRdd.map {
        case ((locOid, itemName), count) => (locOid, (itemName, count))
      }

      // Step 7. Read Dataset B (Locations) and Broadcast ---
      val dfB = spark.read.parquet(inputPathB)

      // Collect as Map and broadcast
      val locationMap = dfB
        .select("geographical_location_oid", "geographical_location")
        .rdd
        .map(row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location")))
        .collectAsMap()

      val broadcastLocMap = spark.sparkContext.broadcast(locationMap)

      // Step 8. Perform Manual Broadcast Hash Join ---
      val joinedRdd: RDD[(Long, String, String, Int)] = prepForJoin.flatMap {
        case (locOid, (itemName, count)) =>
          broadcastLocMap.value.get(locOid).map { locName =>
            (locOid, locName, itemName, count)
          }
      }

      // Step 9. Rank within each location and keep Top X ---
      val finalRankedRdd: RDD[FinalOutput] = joinedRdd
        .groupBy(_._1) // group by loc_oid
        .flatMap { case (locOid, items) =>
          val sorted = items.toSeq.sortBy(-_._4) // sort by count desc
          sorted.zipWithIndex
            .filter { case (_, idx) => idx < topX }
            .map { case ((_, _, itemName, _), idx) =>
              FinalOutput(locOid, (idx + 1).toString, itemName)
            }
        }

      // Step 10. Write Output ---
      val finalDF = finalRankedRdd.toDF("geographical_location_oid", "item_rank", "item_name")
      finalDF.write.mode("overwrite").parquet(outputPath)

      println(s"Manual broadcast hash join complete. Results written to: $outputPath")

    } catch {
      case e: Exception =>
        println(s"An error occurred during Spark job execution: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}

/*
Code snippet on handling data skew
      // Step 9: Phase 1 - Salt the key and distribute the load.
      val NUM_SALT_BUCKETS = 10

      // Key format: ((locOid, salt), (locName, itemName, count))
      val saltedRdd: RDD[((Long, Int), (String, String, Int))] = joinedRdd.map {
        case (locOid, locName, itemName, count) =>
          // FIX 1: Fully qualify Random and ensure NUM_SALT_BUCKETS is in scope
          val salt = scala.util.Random.nextInt(NUM_SALT_BUCKETS)
          ((locOid, salt), (locName, itemName, count))
      }

      // Step 10: Phase 1 - Group and Rank partially by salted key (parallel shuffle).
      // Output: (locOid, itemName, count) - Only for the Top X items from this bucket
      val robustPartiallyRankedRdd: RDD[(Long, String, Int)] = saltedRdd
        .groupByKey()
        .flatMap { case ((locOid, _), items) =>
          val itemsForRanking = items.map { case (_, itemName, count) => (itemName, count) }
          val sorted = itemsForRanking.toSeq.sortBy(-_._2)

          // Return (locOid, itemName, count) - Only for the Top X items from this bucket
          sorted.zipWithIndex
            .filter { case (_, idx) => idx < topX }
            .map { case ((itemName, count), _) => (locOid, itemName, count) }
        }

      // Step 11: Phase 2 - Final Grouping and Ranking (minimal shuffle).
      // FIX 2 & 3: Consolidated logic into one clear assignment to finalRankedRdd
      val finalRankedRdd: RDD[FinalOutput] = robustPartiallyRankedRdd
        .groupBy(_._1) // Group by locOid: RDD[(locOid, Iterable[(locOid, itemName, count)])]
        .flatMap { case (locOid, items) =>

          // 1. Re-aggregate counts (in case an item appeared in multiple salt buckets)
          val aggregatedCounts = items
            .map { case (_, itemName, count) => (itemName, count) } // -> (itemName, count)
            .toList
            .groupBy(_._1) // Group by itemName
            .mapValues(_.map(_._2).sum) // Sum counts for the same item name
            .toSeq

          // 2. Final global sort and rank
          val finalSorted = aggregatedCounts.sortBy(-_._2)

          // 3. Filter Top X and map to FinalOutput
          finalSorted.zipWithIndex
            .filter { case (_, idx) => idx < topX }
            .map { case ((itemName, _), idx) =>
              FinalOutput(locOid, (idx + 1).toString, itemName)
            }
        }
 */
