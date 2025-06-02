package com.data.control.plane

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Random

object SparkETLMetadataListenerExampleV1 {
  def main(args: Array[String]): Unit = {
    println("Starting Enhanced ETL Metadata Listener Example...")

    val spark = SparkSession.builder()
      .appName("ETL Metadata Capture Test")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.sql.files.maxPartitionBytes", "134217728") // 128MB per partition
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Create and register the listener
    val metadataListener = new SparkETLMetadataListener()
    metadataListener.registerWithSparkSession(spark)

    import spark.implicits._

    try {
      println("\n" + "="*80)
      println("STEP 1: CREATING TEST DATA")
      println("="*80)

      // Create larger sales data with more variety for better filtering
      println("\n📊 Creating large sales dataset...")
      val numSalesRecords = 5000000 // 5 million records

      val salesData = spark.range(1, numSalesRecords + 1).mapPartitions { iter =>
        val rand = new Random()
        iter.map { i =>
          val year = 2020 + rand.nextInt(5) // 2020-2024
          val month = 1 + rand.nextInt(12)
          val day = 1 + rand.nextInt(28)
          val amount = year match {
            case 2023 => 500 + rand.nextInt(50000)  // 2023 has higher amounts
            case 2024 => 1000 + rand.nextInt(100000) // 2024 has even higher amounts
            case _ => 100 + rand.nextInt(5000)      // Other years have lower amounts
          }
          val customerId = s"CUST-${1 + rand.nextInt(50000)}" // 50K unique customers
          val productId = s"PROD-${1 + rand.nextInt(1000)}"   // 1000 products
          val storeId = s"STORE-${1 + rand.nextInt(200)}"     // 200 stores
          val region = Seq("North", "South", "East", "West", "Central")(rand.nextInt(5))
          val transactionType = if (amount > 10000) "premium" else "standard"

          (s"TXN-$i", amount, customerId, year, month, productId, storeId, region,
            transactionType, s"$year-${"%02d".format(month)}-${"%02d".format(day)}")
        }
      }.toDF("transaction_id", "amount", "customer_id", "year", "month",
        "product_id", "store_id", "region", "transaction_type", "date")

      // Repartition to create multiple files
      salesData.repartition(20)
        .write
        .mode("overwrite")
        .partitionBy("year", "month")  // Partitioned data
        .parquet("/tmp/sales_partitioned.parquet")

      println(s"✅ Created partitioned sales data: ~${numSalesRecords} records")

      // Create customer data with varied status distribution
      println("\n👥 Creating customer dataset...")
      val numCustomers = 50000

      val customerData = spark.range(1, numCustomers + 1).map { i =>
        val rand = new Random(i) // Seeded for consistency
        val status = i match {
          case n if n % 100 < 5 => "platinum"   // 5% platinum
          case n if n % 100 < 20 => "gold"      // 15% gold
          case n if n % 100 < 50 => "silver"    // 30% silver
          case n if n % 100 < 80 => "bronze"    // 30% bronze
          case _ => "inactive"                   // 20% inactive
        }
        val region = Seq("North", "South", "East", "West", "Central")(i.toInt % 5)
        val joinYear = 2015 + rand.nextInt(10)
        val totalPurchases = status match {
          case "platinum" => 100 + rand.nextInt(500)
          case "gold" => 50 + rand.nextInt(100)
          case "silver" => 20 + rand.nextInt(50)
          case "bronze" => 5 + rand.nextInt(20)
          case _ => rand.nextInt(5)
        }

        (s"CUST-$i", s"Customer $i", status, region, joinYear,
          totalPurchases, s"${i}@example.com", rand.nextDouble() * 100000)
      }.toDF("customer_id", "customer_name", "status", "region",
        "join_year", "total_purchases", "email", "lifetime_value")

      customerData.repartition(10)
        .write
        .mode("overwrite")
        .parquet("/tmp/customers.parquet")

      println(s"✅ Created customer data: ${numCustomers} records")

      // Create product data with categories
      println("\n📦 Creating product dataset...")
      val numProducts = 1000

      val productData = spark.range(1, numProducts + 1).map { i =>
        val category = Seq("Electronics", "Clothing", "Food", "Books",
          "Home", "Sports", "Toys")(i.toInt % 7)
        val subCategory = category match {
          case "Electronics" => Seq("Phones", "Laptops", "Tablets", "Accessories")(i.toInt % 4)
          case "Clothing" => Seq("Men", "Women", "Kids", "Shoes")(i.toInt % 4)
          case _ => "General"
        }
        val price = category match {
          case "Electronics" => 100 + Random.nextInt(2000)
          case "Clothing" => 20 + Random.nextInt(200)
          case _ => 10 + Random.nextInt(100)
        }
        val inventoryCount = Random.nextInt(1000)
        val isActive = i % 10 != 0 // 90% active products

        (s"PROD-$i", s"Product $i", category, subCategory, price.toDouble,
          inventoryCount, isActive, i % 50) // supplier_id
      }.toDF("product_id", "product_name", "category", "sub_category",
        "price", "inventory_count", "is_active", "supplier_id")

      productData.coalesce(5)
        .write
        .mode("overwrite")
        .orc("/tmp/products.orc") // Using ORC format for variety

      println(s"✅ Created product data: ${numProducts} records in ORC format")

      Thread.sleep(3000) // Allow writes to complete

      println("\n" + "="*80)
      println("STEP 2: RUNNING ETL WORKFLOWS WITH FILTERS")
      println("="*80)

      // ETL Workflow 1: High-value recent transactions
      println("\n🚀 ETL Workflow 1: Analyzing high-value recent transactions...")

      val recentHighValueSales = spark.read.parquet("/tmp/sales_partitioned.parquet")
        .filter($"year" >= 2023 && $"amount" > 5000) // Very selective filter
        .filter($"transaction_type" === "premium")
        .select("transaction_id", "amount", "customer_id", "product_id", "date", "region")

      val premiumCustomers = spark.read.parquet("/tmp/customers.parquet")
        .filter($"status".isin("platinum", "gold")) // Only 20% of customers
        .filter($"lifetime_value" > 50000)          // Even more selective
        .select("customer_id", "customer_name", "status", "region", "lifetime_value")

      val expensiveProducts = spark.read.orc("/tmp/products.orc")
        .filter($"category" === "Electronics" && $"price" > 500) // Very selective
        .filter($"is_active" === true)
        .select("product_id", "product_name", "category", "price")

      // Complex join operation
      val premiumAnalysis = recentHighValueSales
        .join(premiumCustomers, Seq("customer_id"), "inner")
        .join(expensiveProducts, Seq("product_id"), "inner")
        .groupBy(recentHighValueSales("region"), $"status", $"category")  // Specify which region
        .agg(
          sum("amount").as("total_revenue"),
          count("transaction_id").as("transaction_count"),
          avg("price").as("avg_product_price"),
          avg("lifetime_value").as("avg_customer_value")
        )
        .filter($"total_revenue" > 100000)

      println("💾 Writing premium analysis results...")
      premiumAnalysis.coalesce(1)
        .write
        .mode("overwrite")
        .parquet("/tmp/premium_analysis.parquet")

      Thread.sleep(2000)

      // ETL Workflow 2: Regional performance analysis
      println("\n🚀 ETL Workflow 2: Regional performance with time-based filters...")

      val q4Sales = spark.read.parquet("/tmp/sales_partitioned.parquet")
        .filter($"year" === 2023 && $"month" >= 10) // Q4 2023 only
        .filter($"amount" > 1000)
        .select("transaction_id", "amount", "customer_id", "store_id", "region")

      val activeCustomers = spark.read.parquet("/tmp/customers.parquet")
        .filter($"status" =!= "inactive")
        .filter($"total_purchases" > 10)
        .select("customer_id", "customer_name", "region", "total_purchases")

      val regionalPerformance = q4Sales
        .join(activeCustomers, Seq("customer_id", "region"), "inner")
        .groupBy(q4Sales("region"), $"store_id")
        .agg(
          sum("amount").as("store_revenue"),
          countDistinct("customer_id").as("unique_customers"),
          sum("total_purchases").as("total_customer_purchases")
        )
        .filter($"store_revenue" > 50000)

      println("💾 Writing regional performance results...")
      regionalPerformance.repartition(4)
        .write
        .mode("overwrite")
        .parquet("/tmp/regional_performance.parquet")

      Thread.sleep(2000)

      // ETL Workflow 3: Product category analysis with multiple filters
      println("\n🚀 ETL Workflow 3: Product category deep dive...")

      val targetCategories = Seq("Electronics", "Clothing")

      val categorySales = spark.read.parquet("/tmp/sales_partitioned.parquet")
        .filter($"year".isin(2023, 2024))
        .filter($"amount" > 500)
        .select("transaction_id", "amount", "product_id", "customer_id", "year")

      val targetProducts = spark.read.orc("/tmp/products.orc")
        .filter($"category".isin(targetCategories: _*))
        .filter($"inventory_count" > 100)
        .filter($"is_active" === true)
        .select("product_id", "category", "sub_category", "price")

      val loyalCustomers = spark.read.parquet("/tmp/customers.parquet")
        .filter($"join_year" <= 2020) // Customers for at least 4 years
        .filter($"status".isin("platinum", "gold", "silver"))
        .select("customer_id", "status", "lifetime_value")

      val categoryAnalysis = categorySales
        .join(targetProducts, "product_id")
        .join(loyalCustomers, "customer_id")
        .groupBy("category", "sub_category", "year", "status")
        .agg(
          sum("amount").as("revenue"),
          count("transaction_id").as("transactions"),
          avg("price").as("avg_price"),
          max("lifetime_value").as("max_customer_value")
        )
        .filter($"revenue" > 10000)
        .orderBy($"revenue".desc)

      println("💾 Writing category analysis results...")
      categoryAnalysis.coalesce(2)
        .write
        .mode("overwrite")
        .orc("/tmp/category_analysis.orc") // Writing as ORC

      Thread.sleep(3000)

      println("\n" + "="*80)
      println("✅ ALL ETL WORKFLOWS COMPLETED!")
      println("="*80)

      println("\nThe metadata listener should have captured:")
      println("1. Source nodes with significant record count reduction after filters")
      println("2. Multiple source files due to partitioning")
      println("3. Clear metrics showing data reduction through the pipeline")
      println("4. Both Parquet and ORC file formats")

    } catch {
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      Thread.sleep(5000) // Final wait for all metadata to be processed

      // Cleanup
      println("\n🧹 Cleaning up test data...")
      try {
        Seq("/tmp/sales_partitioned.parquet", "/tmp/customers.parquet",
          "/tmp/products.orc", "/tmp/premium_analysis.parquet",
          "/tmp/regional_performance.parquet", "/tmp/category_analysis.orc")
          .foreach { path =>
            val p = new org.apache.hadoop.fs.Path(path)
            val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
            if (fs.exists(p)) {
              fs.delete(p, true)
              println(s"  ✓ Deleted $path")
            }
          }
      } catch {
        case e: Exception => println(s"  ✗ Cleanup failed: ${e.getMessage}")
      }

      spark.stop()
    }
  }
}
