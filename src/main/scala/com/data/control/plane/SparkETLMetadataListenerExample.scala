package com.data.control.plane

object SparkETLMetadataListenerExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._

    println("Starting Enhanced ETL Metadata Listener Example...")

    val spark = SparkSession.builder()
      .appName("ETL Metadata Capture Test")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Create and register the listener
    val metadataListener = new SparkETLMetadataListenerV1()
    metadataListener.registerWithSparkSession(spark)

    import spark.implicits._

    try {

      // ETL workflow with metadata capture
      println("\n🚀 Starting ETL workflow...")

      // Read and filter sales
      val sales = spark.read.parquet("/Users/sage/Desktop/code/spark-metadata-listener/src/main/resources/example1/sales")
        .filter($"amount" > 1000 && $"year" === 2023)
        .select("transaction_id", "amount", "customer_id")

      // Read and filter customers
      val customers = spark.read.parquet("/Users/sage/Desktop/code/spark-metadata-listener/src/main/resources/example1/customers")
        .filter($"status" === "active")
        .select("customer_id", "customer_name", "region")

      // Join and aggregate
      val result = sales.join(customers, "customer_id")
        .groupBy("region")
        .agg(
          sum("amount").as("total_sales"),
          countDistinct("customer_id").as("unique_customers")
        )

       //Write results
      println("\n💾 Writing results...")
      result.write
        .mode("overwrite")
        .parquet("/tmp/regional_sales.parquet")

      Thread.sleep(2000)

      // Another write operation
      sales.filter($"amount" > 5000)
        .write
        .mode("overwrite")
        .parquet("/tmp/high_value_sales.parquet")

      Thread.sleep(2000)

      println("\n✅ ETL workflow completed!")

    } catch {
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      Thread.sleep(3000) // Final wait
      spark.stop()
    }
  }
}