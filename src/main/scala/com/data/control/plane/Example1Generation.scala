package com.data.control.plane

object Example1Generation extends  App{
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

  import spark.implicits._

  println("\n📊 Creating sample sales data...")
  val salesData = (1 to 100000).map { i =>
    (
      s"TXN-$i",
      i * 100 + scala.util.Random.nextInt(10000),
      s"CUST-${i % 1000}",
      2020 + (i % 4),
      s"PROD-${i % 50}"
    )
  }.toDF("transaction_id", "amount", "customer_id", "year", "product_id")

  salesData
    .coalesce(4)
    .write
    .mode("overwrite")
    .parquet("/Users/sage/Desktop/code/spark-metadata-listener/src/main/resources/example1/sales")

  println(s"✅ Created sales data: ${salesData.count()} records")

  // Create customer data
  println("\n👥 Creating customer data...")
  val customerData = (1 to 1000).map { i =>
    (
      s"CUST-$i",
      s"Customer $i",
      if (i % 10 < 7) "active" else "inactive",
      Seq("North", "South", "East", "West")(i % 4)
    )
  }.toDF("customer_id", "customer_name", "status", "region")

  customerData
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet("/Users/sage/Desktop/code/spark-metadata-listener/src/main/resources/example1/customers")

  println(s"✅ Created customer data: ${customerData.count()} records")

  Thread.sleep(2000) // Allow listeners to process

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

  val salesCount = spark.read.parquet("/Users/sage/Desktop/code/spark-metadata-listener/src/main/resources/example1/sales").count
  val customersCount = spark.read.parquet("/Users/sage/Desktop/code/spark-metadata-listener/src/main/resources/example1/customers").count
  val salesFilterCount = sales.count
  val customerFilterCount = customers.count

  val result = sales.join(customers, "customer_id")
    .groupBy("region")
    .agg(
      sum("amount").as("total_sales"),
      countDistinct("customer_id").as("unique_customers")
    )


  println(s"salesCount : $salesCount")
  println(s"customersCount : $customersCount")
  println(s"salesFilterCount : $salesFilterCount")
  println(s"customerFilterCount : $customerFilterCount")
  println(s"result : ${result.count}")
  println(s"Sales Filter >5000 : ${sales.filter($"amount" > 5000).count}")

}
