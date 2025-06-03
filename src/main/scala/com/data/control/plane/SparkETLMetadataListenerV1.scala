package com.data.control.plane

import org.apache.spark.scheduler._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommandExec}

import scala.collection.mutable
import java.util.concurrent.ConcurrentHashMap
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

// Data Models (same as before)
case class SourceNodeDetail(
                             tableName: String,
                             folderName: String,
                             sourceType: String,
                             columnsProjected: List[String],
                             filtersApplied: List[String],
                             metricsBeforeFilter: MetricsDetail,
                             metricsAfterFilter: MetricsDetail,
                             partitionsScanned: List[String] = List.empty
                           )

case class TargetNodeDetail(
                             tableName: String,
                             folderName: String,
                             targetType: String,
                             metrics: MetricsDetail
                           )

case class MetricsDetail(
                          recordsCount: Long,
                          bytesProcessed: Long,
                          filesCount: Long = 0,
                          partitionsCount: Long = 0
                        )

case class ActionMetadata(
                           actionId: String,
                           actionType: String,
                           timestamp: String,
                           sourceNodes: List[SourceNodeDetail],
                           targetNode: Option[TargetNodeDetail],
                           executionTimeMs: Long,
                           sparkJobId: Int,
                           sparkStageIds: List[Int]
                         )

// New case class to track stage information
case class StageInfo(
                      stageId: Int,
                      stageName: String,
                      rddName: String,
                      details: String
                    )

class SparkETLMetadataListenerV1 extends SparkListener with QueryExecutionListener {

  // Thread-safe collections
  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()
  private val jobToExecutionId = new ConcurrentHashMap[Int, Long]()
  private val jobToStageMetrics = new ConcurrentHashMap[Int, mutable.Map[Int, StageMetrics]]()
  private val jobStartTimes = new ConcurrentHashMap[Int, Long]()
  private val recentQueryExecutions = new ConcurrentHashMap[Long, QueryExecution]()

  // New: Track stage details
  private val stageIdToInfo = new ConcurrentHashMap[Int, StageInfo]()
  private val stageIdToSourcePath = new ConcurrentHashMap[Int, String]()

  // Enable debug mode
  private var debugMode = false

  private def debug(msg: String): Unit = {
    if (debugMode) println(s"[ETL-DEBUG] $msg")
  }

  case class StageMetrics(
                           stageId: Int,
                           var inputRecords: Long = 0L,
                           var inputBytes: Long = 0L,
                           var outputRecords: Long = 0L,
                           var outputBytes: Long = 0L,
                           var filesRead: Long = 0L,
                           var filesWritten: Long = 0L
                         )

  private var metadataCallback: ActionMetadata => Unit = { metadata =>
    println(s"\n${"=" * 80}")
    println(s"ACTION: ${metadata.actionType} (${metadata.actionId})")
    println(s"${"=" * 80}")

    if (metadata.sourceNodes.nonEmpty) {
      println("\nSOURCE NODES:")
      metadata.sourceNodes.zipWithIndex.foreach { case (source, idx) =>
        println(s"\n  Source ${idx + 1}:")
        println(s"    Table/Path: ${source.tableName}")
        println(s"    Format: ${source.sourceType}")
        println(s"    Projected Columns: ${source.columnsProjected.mkString(", ")}")
        println(s"    Filters: ${if (source.filtersApplied.isEmpty) "None" else source.filtersApplied.mkString(" AND ")}")
        println(s"    Metrics Before Filter:")
        println(s"      - Records: ${source.metricsBeforeFilter.recordsCount}")
        println(s"      - Bytes: ${formatBytes(source.metricsBeforeFilter.bytesProcessed)}")
        println(s"      - Files: ${source.metricsBeforeFilter.filesCount}")
        println(s"    Metrics After Filter:")
        println(s"      - Records: ${source.metricsAfterFilter.recordsCount}")
        println(s"      - Bytes: ${formatBytes(source.metricsAfterFilter.bytesProcessed)}")
      }
    }

    metadata.targetNode.foreach { target =>
      println("\nTARGET NODE:")
      println(s"  Table/Path: ${target.tableName}")
      println(s"  Format: ${target.targetType}")
      println(s"  Metrics:")
      println(s"    - Records Written: ${target.metrics.recordsCount}")
      println(s"    - Bytes Written: ${formatBytes(target.metrics.bytesProcessed)}")
      println(s"    - Files Written: ${target.metrics.filesCount}")
    }

    println(s"\n${"=" * 80}\n")
  }

  def setMetadataCallback(callback: ActionMetadata => Unit): Unit = {
    metadataCallback = callback
  }

  def registerWithSparkSession(spark: SparkSession): Unit = {
    debug("Registering SparkETLMetadataListener")

    // Register as SparkListener
    spark.sparkContext.addSparkListener(this)
    debug("Registered as SparkListener")

    // Register as QueryExecutionListener
    try {
      spark.listenerManager.register(this)
      debug("Registered as QueryExecutionListener")
    } catch {
      case e: Exception =>
        debug(s"Failed to register QueryExecutionListener: ${e.getMessage}")
    }
  }

  // QueryExecutionListener methods
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    debug(s"QueryExecution onSuccess: $funcName")

    // Store the QueryExecution with timestamp
    val timestamp = System.currentTimeMillis()
    recentQueryExecutions.put(timestamp, qe)

    // Clean up old entries (older than 1 minute)
    recentQueryExecutions.asScala.foreach { case (ts, _) =>
      if (timestamp - ts > 60000) {
        recentQueryExecutions.remove(ts)
      }
    }

    // Try to get current execution ID from thread local
    try {
      val executionIdMethod = SQLExecution.getClass.getDeclaredMethod("getExecutionId")
      executionIdMethod.setAccessible(true)
      val executionId = executionIdMethod.invoke(SQLExecution).asInstanceOf[Option[Long]]

      executionId.foreach { id =>
        debug(s"Storing QueryExecution for execution ID: $id")
        executionIdToQueryExecution.put(id, qe)
      }
    } catch {
      case e: Exception =>
        debug(s"Could not get execution ID: ${e.getMessage}")
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    debug(s"QueryExecution failed: $funcName - ${exception.getMessage}")
  }

  // SparkListener methods
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobId = jobStart.jobId
    val executionIdStr = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)

    debug(s"Job $jobId started with ${jobStart.stageIds.length} stages")

    if (executionIdStr != null) {
      try {
        val executionId = executionIdStr.toLong
        jobToExecutionId.put(jobId, executionId)
        debug(s"Mapped job $jobId to execution ID $executionId")
      } catch {
        case e: NumberFormatException =>
          debug(s"Failed to parse execution ID: $executionIdStr")
      }
    }

    jobStartTimes.put(jobId, jobStart.time)

    // Initialize stage metrics
    val stageMetricsMap = mutable.Map[Int, StageMetrics]()
    jobStart.stageIds.foreach { stageId =>
      stageMetricsMap(stageId) = StageMetrics(stageId)
    }
    jobToStageMetrics.put(jobId, stageMetricsMap)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    val stageId = stageInfo.stageId

    // Store stage information
    val info = StageInfo(
      stageId = stageId,
      stageName = stageInfo.name,
      rddName = stageInfo.rddInfos.headOption.map(_.name).getOrElse(""),
      details = stageInfo.details
    )
    stageIdToInfo.put(stageId, info)

    // Try to extract source path from stage name or RDD name
    val pathPattern = """.*?([/\w\-\.]+\.(?:parquet|orc)).*""".r
    val allText = s"${stageInfo.name} ${info.rddName} ${stageInfo.details}"

    pathPattern.findFirstMatchIn(allText).foreach { m =>
      val path = m.group(1)
      stageIdToSourcePath.put(stageId, path)
      debug(s"Stage $stageId associated with path: $path")
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageId = stageInfo.stageId

    debug(s"Stage $stageId completed: ${stageInfo.name}")

    // Add this debug block
    if (debugMode) {
      println(s"[DEBUG] Stage $stageId accumulables:")
      stageInfo.accumulables.foreach { case (id, accum) =>
        println(s"  - ${accum.name.getOrElse("unnamed")}: ${accum.value.getOrElse("null")}")
      }
    }

    // Extract metrics from this specific stage
    val metrics = StageMetrics(stageId)

    // Get metrics from accumulables
    stageInfo.accumulables.foreach { case (_, accum) =>
      accum.name.foreach { name =>
        val value = accum.value.map {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => try { s.toLong } catch { case _: Exception => 0L }
          case _ => 0L
        }.getOrElse(0L)

        name.toLowerCase match {
          case n if n.contains("number of output rows") =>
            metrics.outputRecords = value
          case n if n.contains("number of files read") =>
            metrics.filesRead = value
          case n if n.contains("bytes read") || n.contains("input size") || n.contains("size of files read") =>
            metrics.inputBytes = value
          case n if n.contains("records read") || n.contains("number of input rows") =>
            metrics.inputRecords = value
          case n if n.contains("number of written files") =>
            metrics.filesWritten = value
          case n if n.contains("written output rows") =>
            metrics.outputRecords = value
          case n if n.contains("bytes written") =>
            metrics.outputBytes = value
          case _ =>
        }
      }
    }

    // Also check task metrics
    if (stageInfo.taskMetrics != null) {
      if (metrics.inputRecords == 0) metrics.inputRecords = stageInfo.taskMetrics.inputMetrics.recordsRead
      if (metrics.inputBytes == 0) metrics.inputBytes = stageInfo.taskMetrics.inputMetrics.bytesRead
      if (metrics.outputRecords == 0) metrics.outputRecords = stageInfo.taskMetrics.outputMetrics.recordsWritten
      if (metrics.outputBytes == 0) metrics.outputBytes = stageInfo.taskMetrics.outputMetrics.bytesWritten
    }

    debug(s"Stage $stageId final metrics: input=${metrics.inputRecords}/${formatBytes(metrics.inputBytes)}, output=${metrics.outputRecords}/${formatBytes(metrics.outputBytes)}")

    // Update metrics for all jobs containing this stage
    jobToStageMetrics.asScala.foreach { case (jobId, stageMap) =>
      if (stageMap.contains(stageId)) {
        stageMap(stageId) = metrics
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val executionTime = jobEnd.time - jobStartTimes.getOrDefault(jobId, jobEnd.time)

    debug(s"Job $jobId ended after ${executionTime}ms")

    val executionId = Option(jobToExecutionId.get(jobId))
    val qe = executionId.flatMap(id => Option(executionIdToQueryExecution.get(id)))
      .orElse(findRecentQueryExecution(jobEnd.time))

    val stageMetrics = Option(jobToStageMetrics.get(jobId))

    if (qe.isDefined && stageMetrics.isDefined) {
      debug(s"Found QueryExecution for job $jobId")
      extractAndReportMetadata(jobId, qe.get, stageMetrics.get, executionTime)
    }

    // Cleanup
    jobStartTimes.remove(jobId)
    jobToExecutionId.remove(jobId)
    jobToStageMetrics.remove(jobId)
    executionId.foreach(executionIdToQueryExecution.remove)

    // Clean up stage info for this job's stages
    stageMetrics.foreach { metrics =>
      metrics.keys.foreach { stageId =>
        stageIdToInfo.remove(stageId)
        stageIdToSourcePath.remove(stageId)
      }
    }
  }

  private def findRecentQueryExecution(jobEndTime: Long): Option[QueryExecution] = {
    recentQueryExecutions.asScala
      .filter { case (timestamp, _) => math.abs(timestamp - jobEndTime) < 5000 }
      .toSeq
      .sortBy(_._1)
      .lastOption
      .map(_._2)
  }

  private def extractAndReportMetadata(
                                        jobId: Int,
                                        qe: QueryExecution,
                                        stageMetrics: mutable.Map[Int, StageMetrics],
                                        executionTime: Long
                                      ): Unit = {
    try {
      debug(s"Extracting metadata for job $jobId")

      // Extract sources with proper metrics mapping
      val sources = extractSourceNodesWithMetrics(qe.executedPlan, stageMetrics)
      val target = extractTargetNodeWithMetrics(qe.logical, qe.executedPlan, stageMetrics)

      debug(s"Found ${sources.size} source nodes and ${if (target.isDefined) "1" else "0"} target node")

      // Only report if we have both sources and target
      if (sources.nonEmpty && target.isDefined) {
        val metadata = ActionMetadata(
          actionId = s"job_${jobId}_${System.currentTimeMillis()}",
          actionType = determineActionType(qe.logical),
          timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
          sourceNodes = sources,
          targetNode = target,
          executionTimeMs = executionTime,
          sparkJobId = jobId,
          sparkStageIds = stageMetrics.keys.toList
        )

        metadataCallback(metadata)
      }
    } catch {
      case e: Exception =>
        debug(s"Error extracting metadata: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  private def extractSourceNodesWithMetrics(
                                             plan: SparkPlan,
                                             stageMetrics: mutable.Map[Int, StageMetrics]
                                           ): List[SourceNodeDetail] = {
    val sources = mutable.ListBuffer[SourceNodeDetail]()

    debug("Extracting source nodes with proper metrics")

    // Map to store source path to metrics
    val sourcePathToMetrics = mutable.Map[String, StageMetrics]()

    // First, map stages to source paths
    stageMetrics.foreach { case (stageId, metrics) =>
      stageIdToSourcePath.get(stageId) match {
        case path: String if path.nonEmpty =>
          debug(s"Stage $stageId is associated with source: $path")
          sourcePathToMetrics(path) = metrics
        case _ =>
          // Try to identify from stage info
          val stageInfo = stageIdToInfo.get(stageId)
          if (stageInfo != null) {
            val pathPattern = """.*?([/\w\-\.]+\.(?:parquet|orc)).*""".r
            pathPattern.findFirstMatchIn(stageInfo.rddName).foreach { m =>
              val path = m.group(1)
              sourcePathToMetrics(path) = metrics
            }
          }
      }
    }

    // Now extract source nodes from the plan
    def collectFileSources(node: SparkPlan, depth: Int = 0): Unit = {
      node match {
        case scan: FileSourceScanExec if isParquetOrOrc(scan.relation.fileFormat) =>
          val path = scan.relation.location.rootPaths.headOption.map(_.toString).getOrElse("unknown")
          val tableName = extractTableNameFromPath(path)
          val format = getFileSourceType(scan.relation.fileFormat)
          val columns = scan.output.map(_.name).toList
          val dataFilters = scan.dataFilters.map(_.sql).toList
          val partitionFilters = scan.partitionFilters.map(_.sql).toList
          val allFilters = dataFilters ++ partitionFilters

          // Get metrics specific to this source
          val sourceMetrics = sourcePathToMetrics.find { case (p, _) =>
            path.contains(p) || p.contains(tableName) || tableName.contains(p)
          }.map(_._2).getOrElse {
            // Improved fallback: find the first read stage that hasn't been used
            val readStages = stageMetrics.values.filter(m =>
              m.inputBytes > 0 || m.filesRead > 0
            ).toSeq.sortBy(_.stageId)

            // Try to match by position (first source gets first read stage, etc.)
            val sourceIndex = sources.size
            readStages.lift(sourceIndex).getOrElse(StageMetrics(-1))
          }

          // Calculate initial metrics from scan info
          val selectedPartitions = scan.selectedPartitions
          val files = selectedPartitions.flatMap(_.files)
          val initialBytes = files.map(_.getLen).sum
          val initialFiles = files.length.toLong

          // For before filter: use scan metadata
          // For after filter: use stage output metrics
          val beforeMetrics = MetricsDetail(
            recordsCount = if (sourceMetrics.inputRecords > 0) sourceMetrics.inputRecords else 0L,
            bytesProcessed = if (initialBytes > 0) initialBytes else sourceMetrics.inputBytes,
            filesCount = if (initialFiles > 0) initialFiles else sourceMetrics.filesRead
          )

          val afterMetrics = MetricsDetail(
            recordsCount = sourceMetrics.outputRecords,
            bytesProcessed = sourceMetrics.outputBytes,
            filesCount = sourceMetrics.filesRead
          )

          debug(s"Source $tableName - Before: ${beforeMetrics.recordsCount} records, After: ${afterMetrics.recordsCount} records")

          sources += SourceNodeDetail(
            tableName = tableName,
            folderName = path,
            sourceType = format,
            columnsProjected = columns,
            filtersApplied = allFilters,
            metricsBeforeFilter = beforeMetrics,
            metricsAfterFilter = afterMetrics,
            partitionsScanned = selectedPartitions.take(5).map(_.toString).toList
          )

        case _ =>
      }

      // Recursively process children
      node.children.foreach(child => collectFileSources(child, depth + 1))
    }

    collectFileSources(plan)
    sources.toList
  }

  private def extractTargetNodeWithMetrics(
                                            logical: LogicalPlan,
                                            physical: SparkPlan,
                                            stageMetrics: mutable.Map[Int, StageMetrics]
                                          ): Option[TargetNodeDetail] = {
    // For target, we want the write stage metrics (usually the last stage)
    val writeStageMetrics = stageMetrics.values
      .filter(m => m.outputRecords > 0 || m.filesWritten > 0)
      .toSeq
      .sortBy(_.stageId)
      .lastOption
      .getOrElse(StageMetrics(-1))

    debug(s"Target metrics: ${writeStageMetrics.outputRecords} records, ${formatBytes(writeStageMetrics.outputBytes)}")

    // Check physical plan first
    physical.collectFirst {
      case DataWritingCommandExec(cmd, _) => cmd
    }.flatMap {
      case insert: InsertIntoHadoopFsRelationCommand =>
        Some(TargetNodeDetail(
          tableName = extractTableNameFromPath(insert.outputPath.toString),
          folderName = insert.outputPath.toString,
          targetType = getFileSourceType(insert.fileFormat),
          metrics = MetricsDetail(
            recordsCount = writeStageMetrics.outputRecords,
            bytesProcessed = writeStageMetrics.outputBytes,
            filesCount = if (writeStageMetrics.filesWritten > 0) writeStageMetrics.filesWritten else 1L
          )
        ))
      case _ => None
    }.orElse {
      // Check logical plan
      logical.collectFirst {
        case save: SaveIntoDataSourceCommand =>
          val path = save.options.getOrElse("path", "unknown")
          TargetNodeDetail(
            tableName = extractTableNameFromPath(path),
            folderName = path,
            targetType = "PARQUET_FILE",
            metrics = MetricsDetail(
              recordsCount = writeStageMetrics.outputRecords,
              bytesProcessed = writeStageMetrics.outputBytes,
              filesCount = if (writeStageMetrics.filesWritten > 0) writeStageMetrics.filesWritten else 1L
            )
          )
        case create: CreateDataSourceTableAsSelectCommand =>
          val path = create.table.storage.locationUri.map(_.toString).getOrElse("unknown")
          TargetNodeDetail(
            tableName = create.table.identifier.table,
            folderName = path,
            targetType = create.table.provider.getOrElse("UNKNOWN"),
            metrics = MetricsDetail(
              recordsCount = writeStageMetrics.outputRecords,
              bytesProcessed = writeStageMetrics.outputBytes,
              filesCount = if (writeStageMetrics.filesWritten > 0) writeStageMetrics.filesWritten else 1L
            )
          )
      }
    }
  }

  // Helper methods (same as before)
  private def isParquetOrOrc(fileFormat: FileFormat): Boolean = {
    fileFormat match {
      case _: ParquetFileFormat => true
      case _: OrcFileFormat => true
      case _ => false
    }
  }

  private def getFileSourceType(fileFormat: FileFormat): String = {
    fileFormat match {
      case _: ParquetFileFormat => "PARQUET_FILE"
      case _: OrcFileFormat => "ORC_FILE"
      case _ => "UNKNOWN_FILE"
    }
  }

  private def extractTableNameFromPath(path: String): String = {
    val p = new Path(path)
    p.getName.replaceAll("\\.(parquet|orc)$", "")
  }

  private def determineActionType(plan: LogicalPlan): String = {
    plan match {
      case _: Command => "WRITE_COMMAND"
      case _ if plan.find(_.isInstanceOf[InsertIntoStatement]).isDefined => "INSERT"
      case _ if plan.find(_.isInstanceOf[CreateTableAsSelect]).isDefined => "CREATE_TABLE_AS_SELECT"
      case _ => "TRANSFORMATION"
    }
  }

  private def formatBytes(bytes: Long): String = {
    if (bytes <= 0) "0 B"
    else if (bytes < 1024) s"$bytes B"
    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.2f KB"
    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024)}%.2f MB"
    else f"${bytes / (1024.0 * 1024 * 1024)}%.2f GB"
  }
}

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
