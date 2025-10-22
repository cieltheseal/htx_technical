import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

object GenerateParquet {

  def main(args: Array[String]): Unit = {

    val hadoopHomeDir = "hadoop 3.3.6"
    System.setProperty("hadoop.home.dir", hadoopHomeDir)
    System.setProperty("java.library.path", hadoopHomeDir + "\\bin")

    // 1. Initialize Spark Session
    // .master("local[*]") allows Spark to run locally using all available cores.
    val spark = SparkSession.builder()
      .appName("SmallParquetGenerator")
      .master("local[*]")
      .config("spark.hadoop.fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .getOrCreate()

    import spark.implicits._

    val outputDir = "data/input"

    // Generate Dataset A (1 million rows)
    println("Generating Dataset A (1 million rows)...")
    val datasetA = generateDatasetA(spark, 1000000)
    datasetA.write
      .mode("overwrite")
      .parquet(s"$outputDir/dataset_a.parquet")
    println(s"Dataset A saved to $outputDir/dataset_a.parquet")

    // Generate Dataset B (10,000 rows)
    println("Generating Dataset B (10,000 rows)...")
    val datasetB = generateDatasetB(spark, 10000)
    datasetB.write
      .mode("overwrite")
      .parquet(s"$outputDir/dataset_b.parquet")
    println(s"Dataset B saved to $outputDir/dataset_b.parquet")

    // Show sample data
    println("\nDataset A Sample:")
    datasetA.show(5, truncate = false)

    println("\nDataset B Sample:")
    datasetB.show(5, truncate = false)

    spark.stop()
  }

  def generateDatasetA(spark: SparkSession, numRows: Int): DataFrame = {
    import spark.implicits._

    val random = new Random(1)

    // Item names to randomly select from
    val itemNames = Seq(
      "Person", "Car", "Bicycle", "Motorcycle", "Bus", "Truck",
      "Traffic Light", "Stop Sign", "Backpack", "Umbrella", "Handbag",
      "Suitcase", "Dog", "Cat", "Bird", "Horse", "Sheep", "Cow",
      "Bottle", "Cup", "Fork", "Knife", "Spoon", "Bowl", "Banana",
      "Apple", "Orange", "Pizza", "Donut", "Cake", "Chair", "Couch",
      "Potted Plant", "Bed", "Dining Table", "Laptop", "Mouse", "Phone",
      "Keyboard", "Book", "Clock", "Vase", "Scissors", "Teddy Bear"
    )

    // Generate data in partitions for better performance
    val partitions = 10
    val rowsPerPartition = numRows / partitions

    val data = (0 until partitions).flatMap { partition =>
      (0 until rowsPerPartition).map { i =>
        val idx = partition * rowsPerPartition + i
        val geoLocationOid = random.nextInt(10000).toLong + 1L
        val videoCameraOid = random.nextInt(5000).toLong + 1L
        val detectionOid = idx.toLong + 1L
        val itemName = itemNames(random.nextInt(itemNames.length))
        val timestampDetected = System.currentTimeMillis() - random.nextInt(86400000) * 30 // Last 30 days

        (geoLocationOid, videoCameraOid, detectionOid, itemName, timestampDetected)
      }
    }

    data.toDF(
      "geographical_location_oid",
      "video_camera_oid",
      "detection_oid",
      "item_name",
      "timestamp_detected"
    )
  }

  def generateDatasetB(spark: SparkSession, numRows: Int): DataFrame = {
    import spark.implicits._

    val random = new Random(42)

    // Sample geographical locations in Singapore
    val areas = Seq(
      "Ang Mo Kio", "Bedok", "Bishan", "Bukit Batok", "Bukit Merah",
      "Bukit Panjang", "Bukit Timah", "Choa Chu Kang", "Clementi", "Geylang",
      "Hougang", "Jurong East", "Jurong West", "Kallang", "Marine Parade",
      "Pasir Ris", "Punggol", "Queenstown", "Sembawang", "Sengkang",
      "Serangoon", "Tampines", "Toa Payoh", "Woodlands", "Yishun",
      "Admiralty", "Boon Lay", "Changi", "Holland Village", "Joo Chiat",
      "Katong", "Lavender", "Marsiling", "Newton", "Novena",
      "Orchard", "Paya Lebar", "Potong Pasir", "River Valley", "Simei"
    )

    val streets = Seq(
      "Avenue", "Road", "Street", "Drive", "Lane", "Crescent", "Park",
      "View", "Link", "Walk", "Central", "Place", "Close", "Terrace",
      "Rise", "Grove", "Way", "Heights", "Garden", "Circuit"
    )

    val data = (1L to numRows.toLong).map { oid =>
      val area = areas(random.nextInt(areas.length))
      val streetType = streets(random.nextInt(streets.length))
      val streetNumber = random.nextInt(999) + 1
      val unitNumber = if (random.nextBoolean()) {
        val floor = "%02d".format(random.nextInt(20) + 1)
        val unit = "%03d".format(random.nextInt(999) + 1)
        s" #$floor-$unit"
      } else ""
      val postalCode = random.nextInt(800000) + 100000 // Singapore postal codes: 100000-900000
      val geoLocation = s"$streetNumber $area $streetType$unitNumber, Singapore $postalCode"

      (oid, geoLocation)
    }

    data.toDF(
      "geographical_location_oid",
      "geographical_location"
    )
  }
}