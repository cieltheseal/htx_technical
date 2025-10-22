package com.htx

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import com.htx.FinalOutput

/**
 * Unit Test for Item Ranking logic.
 *
 * This test suite uses BeforeAndAfterEach to create and stop a fresh, isolated
 * SparkSession for every single test case, which is the most reliable way to
 * prevent "SparkContext stopped" errors in test environments.
 */
class ItemRankingUnitTest extends AnyFunSuite with BeforeAndAfterEach {
  var spark: SparkSession = _

  // --- SETUP: Create a new SparkSession before each test ---
  override def beforeEach(): Unit = {
    spark = SparkSession
      .builder()
      .appName("Spark Test")
      .master("local[*]") // Use all available cores for testing
      .config("spark.ui.enabled", "false") // Disable UI for faster startup
      .getOrCreate()
    super.beforeEach()
  }

  // --- TEARDOWN: Stop the SparkSession after each test ---
  override def afterEach(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterEach()
    }
  }

  test("Deduplication and counting logic should produce correct item counts per location") {
    val s = spark
    import s.implicits._

    // Sample DataFrame simulating input A
    val dfA = Seq(
      (1L, 101L, 1001L, "Apple", 1697625600L),
      (1L, 101L, 1002L, "Apple", 1697625660L), // same location, different detection
      (1L, 101L, 1001L, "Apple", 1697625661L), // duplicate record
      (1L, 102L, 1003L, "Banana", 1697625720L),
      (2L, 201L, 2001L, "Pear", 1697625780L),
      (2L, 202L, 2002L, "Apple", 1697625840L)
    ).toDF("geographical_location_oid", "video_camera_oid", "detection_oid", "item_name", "timestamp_detected")


    val rddA_prepped = dfA.select("geographical_location_oid", "detection_oid", "item_name").rdd
    val initialMappedRdd = rddA_prepped.map { row =>
      (row.getAs[Long]("detection_oid"), (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("item_name")))
    }

    // Deduplicate by detection_oid and then map to (locOid, itemName)
    val uniqueDetections = initialMappedRdd
      .reduceByKey((a, _) => a)
      .values

    // Count by ((locOid, itemName), count)
    val countsRdd = uniqueDetections
      .map(key => (key, 1))
      .reduceByKey(_ + _)

    val results = countsRdd.collect().toSet

    val expected = Set(
      ((1L, "Apple"), 2),   // (loc 1, Apple) count is 2 (1001 + 1002)
      ((1L, "Banana"), 1),
      ((2L, "Apple"), 1),
      ((2L, "Pear"), 1)
    )

    assert(results == expected)
  }

  test("Top X ranking should correctly assign rank by descending count") {
    val s = spark
    import s.implicits._

    val topX = 2

    // Simulate the aggregated data structure: (locOid, locName, itemName, count)
    val joinedRdd: RDD[(Long, String, String, Int)] = s.sparkContext.parallelize(Seq(
      (1L, "Singapore", "Apple", 5),
      (1L, "Singapore", "Banana", 3),
      (1L, "Singapore", "Orange", 2),
      (2L, "Malaysia", "Car", 10),
      (2L, "Malaysia", "Bike", 15)
    ))

    // Replicate the logic to calculate top X by location
    val finalRankedRdd: RDD[FinalOutput] = joinedRdd
      .groupBy(_._1) // Group by loc_oid
      .flatMap { case (locOid, items) =>
        val sorted = items.toSeq.sortBy(-_._4) // sort by count desc (field 4)
        sorted.zipWithIndex
          .filter { case (_, idx) => idx < topX } // keep top X
          .map { case ((_, _, itemName, _), idx) =>
            // Create the FinalOutput with rank (idx + 1)
            FinalOutput(locOid, (idx + 1).toString, itemName)
          }
      }

    val result = finalRankedRdd.collect().toSet

    // Expected output: Top 2 for loc 1 (Apple 5, Banana 3) and Top 2 for loc 2 (Bike 15, Car 10)
    val expected = Set(
      FinalOutput(1L, "1", "Apple"),
      FinalOutput(1L, "2", "Banana"),
      FinalOutput(2L, "1", "Bike"),
      FinalOutput(2L, "2", "Car")
    )

    assert(result == expected)
  }
}
