package com.htx

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import com.htx.FinalOutput
import scala.jdk.CollectionConverters._
import scala.util.Try
import org.scalatest.funsuite.AnyFunSuite

class ItemRankingIntegrationTest extends AnyFunSuite {

  // Define shared path constants
  private val basePath = "./spark_test_artifacts"
  private val topX = 2


  // =========================================================================
  // TEST: CORE RANKING LOGIC (FULL INTEGRATION)
  // =========================================================================
  test("Verify Core Ranking Logic and TopX Limit for Locations 10L and 20L") {

    // --- Setup Spark Session 1 (Writer/Job Runner) ---
    // This session is isolated and is expected to be stopped by ItemRanking.main
    val sparkWriter = SparkSession.builder()
      .appName("ItemRankingWriter")
      .master("local[*]")
      .config("spark.hadoop.hadoop.security.authentication", "simple")
      .getOrCreate()

    sparkWriter.sparkContext.setLogLevel("ERROR")

    // Paths specific to this test run
    val inputPathA = s"$basePath/inputA"
    val inputPathB = s"$basePath/inputB"
    val outputPath = s"$basePath/output

    Files.createDirectories(Paths.get(basePath))

    val sparkImplicits = sparkWriter.implicits
    import sparkImplicits._

    // 3️⃣ Prepare synthetic input data (RDD-CENTRIC CREATION)

    // Input A: detections
    val dataA = Seq(
      (10L, 1001L, 1L, "ItemX", 1678886400L), (10L, 1002L, 2L, "ItemX", 1678886401L),
      (10L, 1001L, 10L, "ItemX", 1678886409L), (10L, 1004L, 11L, "ItemX", 1678886410L),
      (10L, 1004L, 12L, "ItemX", 1678886411L), // ItemX (5)
      (10L, 1003L, 4L, "ItemY", 1678886403L), (10L, 1003L, 5L, "ItemY", 1678886404L),
      (10L, 1005L, 13L, "ItemY", 1678886412L), // ItemY (3)
      (10L, 1001L, 6L, "ItemZ", 1678886405L), // ItemZ (1)

      (20L, 2001L, 7L, "ItemA", 1678886406L), // ItemA (1)
      (20L, 2002L, 8L, "ItemB", 1678886407L), (20L, 2002L, 9L, "ItemB", 1678886408L) // ItemB (2)
    )
    val schemaA = StructType(Seq(
      StructField("geographical_location_oid", LongType, false),
      StructField("video_camera_oid", LongType, false),
      StructField("detection_oid", LongType, false),
      StructField("item_name", StringType, false),
      StructField("timestamp_detected", LongType, false)
    ))
    val rddA = sparkWriter.sparkContext.parallelize(dataA.map(Row.fromTuple))
    val dfA = sparkWriter.createDataFrame(rddA, schemaA)

    dfA.write.mode("overwrite").parquet(inputPathA)

    // Input B: locations
    val dataB = Seq(
      (10L, "New York"),
      (20L, "London"),
      (30L, "Tokyo")
    )
    val schemaB = StructType(Seq(
      StructField("geographical_location_oid", LongType, false),
      StructField("geographical_location", StringType, false)
    ))
    val rddB = sparkWriter.sparkContext.parallelize(dataB.map(Row.fromTuple))
    val dfB = sparkWriter.createDataFrame(rddB, schemaB)

    dfB.write.mode("overwrite").parquet(inputPathB)

    // 4️⃣ Run the actual ItemRanking job
    ItemRanking.main(Array(inputPathA, inputPathB, outputPath, topX.toString))

    // 5️⃣ Read and verify output
    var sparkReader: SparkSession = null
    try {
      sparkReader = SparkSession.builder()
        .appName("ItemRankingReader")
        .master("local[*]")
        .getOrCreate()

      val readerImplicits = sparkReader.implicits
      import readerImplicits._

      val resultDF = sparkReader.read.parquet(outputPath).as[FinalOutput]
      val resultSet = resultDF.collect().toSet

      // --- Start of Ranking Assertions ---
      val loc10Results = resultSet.filter(_.geographical_location_oid == 10L)
      val loc20Results = resultSet.filter(_.geographical_location_oid == 20L)

      // Assertion 1: Check Rank 1 for location 10L (Expected: ItemX)
      val rank1Result_10L = loc10Results.find(_.item_rank == "1")
      assert(rank1Result_10L.isDefined, "Loc 10 must have a rank 1 result.")
      assert(
        rank1Result_10L.get.item_name == "ItemX",
        s"Loc 10 Rank 1 failure. Expected ItemX, Found: ${rank1Result_10L.get.item_name}"
      )

      // Assertion 2: Check Rank 2 for location 10L (Expected: ItemY)
      val rank2Result_10L = loc10Results.find(_.item_rank == "2")
      assert(rank2Result_10L.isDefined, "Loc 10 must have a rank 2 result.")
      assert(rank2Result_10L.get.item_name == "ItemY", "Loc 10 Rank 2 failure. Expected ItemY.")

      // Assertion 3: Check TopX limit (must only have 2 results for 10L)
      assert(loc10Results.size == 2, s"Loc 10 must only have 2 results (TopX=2). Found: ${loc10Results.size}")


      // Assertion 4: Check Rank 1 for location 20L (Expected: ItemB)
      val rank1Result_20L = loc20Results.find(_.item_rank == "1")
      assert(rank1Result_20L.isDefined, "Loc 20 must have a rank 1 result.")
      assert(rank1Result_20L.get.item_name == "ItemB", "Loc 20 Rank 1 failure. Expected ItemB.")

      // Assertion 5: Check Rank 2 for location 20L (Expected: ItemA)
      val rank2Result_20L = loc20Results.find(_.item_rank == "2")
      assert(rank2Result_20L.isDefined, "Loc 20 must have a rank 2 result.")
      assert(rank2Result_20L.get.item_name == "ItemA", "Loc 20 Rank 2 failure. Expected ItemA.")

      // Assertion 6: Check TopX limit (must only have 2 results for 20L)
      assert(loc20Results.size == 2, s"Loc 20 must only have 2 results (TopX=2). Found: ${loc20Results.size}")
      // --- End of Ranking Assertions ---
    } finally {
      // Clean up the reader session
      if (sparkReader != null) {
        sparkReader.stop()
      }
    }
  }
}
