//package com.data.control.plane
//
//import org.apache.spark.scheduler._
//import org.apache.spark.sql.catalyst.plans.logical._
//import org.apache.spark.sql.execution._
//import org.apache.spark.sql.execution.datasources._
//import org.apache.spark.sql.execution.datasources.v2._
//import org.apache.spark.sql.execution.command._
//import org.apache.spark.sql.catalyst.expressions._
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.util.QueryExecutionListener
//import org.apache.hadoop.fs.Path
//import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
//import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
//import org.apache.spark.sql.execution.SQLExecution
//
//import scala.collection.mutable
//import java.util.concurrent.ConcurrentHashMap
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//import scala.collection.JavaConverters._
//
//// Data Models
//case class SourceNodeDetail(
//                             tableName: String,
//                             folderName: String,
//                             sourceType: String,
//                             columnsProjected: List[String],
//                             filtersApplied: List[String],
//                             metricsBeforeFilter: MetricsDetail,
//                             metricsAfterFilter: MetricsDetail,
//                             partitionsScanned: List[String] = List.empty
//                           )
//
//case class TargetNodeDetail(
//                             tableName: String,
//                             folderName: String,
//                             targetType: String,
//                             metrics: MetricsDetail
//                           )
//
//case class MetricsDetail(
//                          recordsCount: Long,
//                          bytesProcessed: Long,
//                          filesCount: Long = 0,
//                          partitionsCount: Long = 0
//                        )
//
//case class ActionMetadata(
//                           actionId: String,
//                           actionType: String,
//                           timestamp: String,
//                           sourceNodes: List[SourceNodeDetail],
//                           targetNode: Option[TargetNodeDetail],
//                           executionTimeMs: Long,
//                           sparkJobId: Int,
//                           sparkStageIds: List[Int]
//                         )
//
//class SparkETLMetadataListener extends SparkListener with QueryExecutionListener {
//
//  // Thread-safe collections
//  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()
//  private val jobToExecutionId = new ConcurrentHashMap[Int, Long]()
//  private val jobToStageMetrics = new ConcurrentHashMap[Int, mutable.Map[Int, StageMetrics]]()
//  private val jobStartTimes = new ConcurrentHashMap[Int, Long]()
//  private val recentQueryExecutions = new ConcurrentHashMap[Long, QueryExecution]()
//
//  // Enable debug mode
//  private var debugMode = false
//
//  private def debug(msg: String): Unit = {
//    if (debugMode) println(s"[ETL-DEBUG] $msg")
//  }
//
//  case class StageMetrics(
//                           stageId: Int,
//                           var inputRecords: Long = 0L,
//                           var inputBytes: Long = 0L,
//                           var outputRecords: Long = 0L,
//                           var outputBytes: Long = 0L,
//                           var filesRead: Long = 0L,
//                           var filesWritten: Long = 0L
//                         )
//
//  private var metadataCallback: ActionMetadata => Unit = { metadata =>
//    println(s"\n${"=" * 80}")
//    println(s"ACTION: ${metadata.actionType} (${metadata.actionId})")
//    println(s"${"=" * 80}")
//
//    if (metadata.sourceNodes.nonEmpty) {
//      println("\nSOURCE NODES:")
//      metadata.sourceNodes.zipWithIndex.foreach { case (source, idx) =>
//        println(s"\n  Source ${idx + 1}:")
//        println(s"    Table/Path: ${source.tableName}")
//        println(s"    Format: ${source.sourceType}")
//        println(s"    Projected Columns: ${source.columnsProjected.mkString(", ")}")
//        println(s"    Filters: ${if (source.filtersApplied.isEmpty) "None" else source.filtersApplied.mkString(" AND ")}")
//        println(s"    Metrics Before Filter:")
//        println(s"      - Records: ${source.metricsBeforeFilter.recordsCount}")
//        println(s"      - Bytes: ${formatBytes(source.metricsBeforeFilter.bytesProcessed)}")
//        println(s"      - Files: ${source.metricsBeforeFilter.filesCount}")
//        println(s"    Metrics After Filter:")
//        println(s"      - Records: ${source.metricsAfterFilter.recordsCount}")
//        println(s"      - Bytes: ${formatBytes(source.metricsAfterFilter.bytesProcessed)}")
//      }
//    }
//
//    metadata.targetNode.foreach { target =>
//      println("\nTARGET NODE:")
//      println(s"  Table/Path: ${target.tableName}")
//      println(s"  Format: ${target.targetType}")
//      println(s"  Metrics:")
//      println(s"    - Records Written: ${target.metrics.recordsCount}")
//      println(s"    - Bytes Written: ${formatBytes(target.metrics.bytesProcessed)}")
//      println(s"    - Files Written: ${target.metrics.filesCount}")
//    }
//
//    println(s"\n${"=" * 80}\n")
//  }
//
//  def setMetadataCallback(callback: ActionMetadata => Unit): Unit = {
//    metadataCallback = callback
//  }
//
//  def registerWithSparkSession(spark: SparkSession): Unit = {
//    debug("Registering SparkETLMetadataListener")
//
//    // Register as SparkListener
//    spark.sparkContext.addSparkListener(this)
//    debug("Registered as SparkListener")
//
//    // Register as QueryExecutionListener
//    try {
//      spark.listenerManager.register(this)
//      debug("Registered as QueryExecutionListener")
//    } catch {
//      case e: Exception =>
//        debug(s"Failed to register QueryExecutionListener: ${e.getMessage}")
//    }
//  }
//
//  // QueryExecutionListener methods
//  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
//    debug(s"QueryExecution onSuccess: $funcName")
//
//    // Store the QueryExecution with timestamp
//    val timestamp = System.currentTimeMillis()
//    recentQueryExecutions.put(timestamp, qe)
//
//    // Clean up old entries (older than 1 minute)
//    recentQueryExecutions.asScala.foreach { case (ts, _) =>
//      if (timestamp - ts > 60000) {
//        recentQueryExecutions.remove(ts)
//      }
//    }
//
//    // Try to get current execution ID from thread local
//    try {
//      val executionIdMethod = SQLExecution.getClass.getDeclaredMethod("getExecutionId")
//      executionIdMethod.setAccessible(true)
//      val executionId = executionIdMethod.invoke(SQLExecution).asInstanceOf[Option[Long]]
//
//      executionId.foreach { id =>
//        debug(s"Storing QueryExecution for execution ID: $id")
//        executionIdToQueryExecution.put(id, qe)
//      }
//    } catch {
//      case e: Exception =>
//        debug(s"Could not get execution ID: ${e.getMessage}")
//    }
//  }
//
//  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
//    debug(s"QueryExecution failed: $funcName - ${exception.getMessage}")
//  }
//
//  // SparkListener methods
//  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
//    val jobId = jobStart.jobId
//    val executionIdStr = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
//
//    debug(s"Job $jobId started, SQL execution ID string: $executionIdStr")
//
//    if (executionIdStr != null) {
//      try {
//        val executionId = executionIdStr.toLong
//        jobToExecutionId.put(jobId, executionId)
//        debug(s"Mapped job $jobId to execution ID $executionId")
//      } catch {
//        case e: NumberFormatException =>
//          debug(s"Failed to parse execution ID: $executionIdStr")
//      }
//    }
//
//    jobStartTimes.put(jobId, jobStart.time)
//
//    // Initialize stage metrics
//    val stageMetricsMap = mutable.Map[Int, StageMetrics]()
//    jobStart.stageIds.foreach { stageId =>
//      stageMetricsMap(stageId) = StageMetrics(stageId)
//    }
//    jobToStageMetrics.put(jobId, stageMetricsMap)
//  }
//
//  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
//    val stageInfo = stageCompleted.stageInfo
//    val stageId = stageInfo.stageId
//
//    debug(s"Stage $stageId completed")
//
//    // Extract metrics from accumulables
//    val metrics = StageMetrics(stageId)
//
//    stageInfo.accumulables.foreach { case (_, accum) =>
//      accum.name.foreach { name =>
//        val value = accum.value.map {
//          case l: Long => l
//          case i: Int => i.toLong
//          case s: String => try { s.toLong } catch { case _: Exception => 0L }
//          case _ => 0L
//        }.getOrElse(0L)
//
//        name.toLowerCase match {
//          case n if n.contains("number of output rows") =>
//            metrics.outputRecords = math.max(metrics.outputRecords, value)
//          case n if n.contains("number of files read") =>
//            metrics.filesRead = math.max(metrics.filesRead, value)
//          case n if n.contains("bytes read") || n.contains("input size") =>
//            metrics.inputBytes = math.max(metrics.inputBytes, value)
//          case n if n.contains("records read") =>
//            metrics.inputRecords = math.max(metrics.inputRecords, value)
//          case n if n.contains("number of written files") =>
//            metrics.filesWritten = math.max(metrics.filesWritten, value)
//          case n if n.contains("written output rows") =>
//            metrics.outputRecords = math.max(metrics.outputRecords, value)
//          case n if n.contains("bytes written") =>
//            metrics.outputBytes = math.max(metrics.outputBytes, value)
//          case _ =>
//        }
//      }
//    }
//
//    // Also check task metrics
//    if (stageInfo.taskMetrics != null) {
//      metrics.inputRecords = math.max(metrics.inputRecords, stageInfo.taskMetrics.inputMetrics.recordsRead)
//      metrics.inputBytes = math.max(metrics.inputBytes, stageInfo.taskMetrics.inputMetrics.bytesRead)
//      metrics.outputRecords = math.max(metrics.outputRecords, stageInfo.taskMetrics.outputMetrics.recordsWritten)
//      metrics.outputBytes = math.max(metrics.outputBytes, stageInfo.taskMetrics.outputMetrics.bytesWritten)
//    }
//
//    debug(s"Stage $stageId metrics: input=${metrics.inputRecords}/${metrics.inputBytes}, output=${metrics.outputRecords}/${metrics.outputBytes}")
//
//    // Update all jobs containing this stage
//    jobToStageMetrics.asScala.foreach { case (jobId, stageMap) =>
//      if (stageMap.contains(stageId)) {
//        stageMap(stageId) = metrics
//      }
//    }
//  }
//
//  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
//    val jobId = jobEnd.jobId
//    val executionTime = jobEnd.time - jobStartTimes.getOrDefault(jobId, jobEnd.time)
//
//    debug(s"Job $jobId ended after ${executionTime}ms")
//
//    val executionId = Option(jobToExecutionId.get(jobId))
//    val qe = executionId.flatMap(id => Option(executionIdToQueryExecution.get(id)))
//      .orElse(findRecentQueryExecution(jobEnd.time))
//
//    val stageMetrics = Option(jobToStageMetrics.get(jobId))
//
//    if (qe.isDefined && stageMetrics.isDefined) {
//      debug(s"Found QueryExecution for job $jobId")
//      extractAndReportMetadata(jobId, qe.get, stageMetrics.get, executionTime)
//    } else if (stageMetrics.isDefined) {
//      debug(s"No QueryExecution found for job $jobId, but have stage metrics")
//      val aggregated = aggregateStageMetrics(stageMetrics.get)
//      if (aggregated.outputRecords > 0 || aggregated.outputBytes > 0) {
//        createBasicWriteMetadata(jobId, stageMetrics.get, executionTime)
//      }
//    } else {
//      debug(s"No data available for job $jobId")
//    }
//
//    // Cleanup
//    jobStartTimes.remove(jobId)
//    jobToExecutionId.remove(jobId)
//    jobToStageMetrics.remove(jobId)
//    executionId.foreach(executionIdToQueryExecution.remove)
//  }
//
//  private def findRecentQueryExecution(jobEndTime: Long): Option[QueryExecution] = {
//    // Find the most recent QueryExecution within 5 seconds of job end
//    recentQueryExecutions.asScala
//      .filter { case (timestamp, _) => math.abs(timestamp - jobEndTime) < 5000 }
//      .toSeq
//      .sortBy(_._1)
//      .lastOption
//      .map(_._2)
//  }
//
//  private def extractAndReportMetadata(
//                                        jobId: Int,
//                                        qe: QueryExecution,
//                                        stageMetrics: mutable.Map[Int, StageMetrics],
//                                        executionTime: Long
//                                      ): Unit = {
//    try {
//      debug(s"Extracting metadata for job $jobId")
//      debug(s"Logical plan class: ${qe.logical.getClass.getSimpleName}")
//      debug(s"Physical plan class: ${qe.executedPlan.getClass.getSimpleName}")
//
//      val sources = extractSourceNodes(qe.executedPlan, stageMetrics)
//      val target = extractTargetNode(qe.logical, qe.executedPlan, stageMetrics)
//
//      debug(s"Found ${sources.size} source nodes and ${if (target.isDefined) "1" else "0"} target node")
//
//      if (sources.nonEmpty || target.isDefined) {
//        val metadata = ActionMetadata(
//          actionId = s"job_${jobId}_${System.currentTimeMillis()}",
//          actionType = determineActionType(qe.logical),
//          timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
//          sourceNodes = sources,
//          targetNode = target,
//          executionTimeMs = executionTime,
//          sparkJobId = jobId,
//          sparkStageIds = stageMetrics.keys.toList
//        )
//
//        metadataCallback(metadata)
//      }
//    } catch {
//      case e: Exception =>
//        debug(s"Error extracting metadata: ${e.getMessage}")
//        e.printStackTrace()
//    }
//  }
//
//  private def extractSourceNodes(
//                                  plan: SparkPlan,
//                                  stageMetrics: mutable.Map[Int, StageMetrics]
//                                ): List[SourceNodeDetail] = {
//    val sources = mutable.ListBuffer[SourceNodeDetail]()
//
//    debug("Searching for source nodes in physical plan")
//
//    // Recursively search for FileSourceScanExec nodes
//    def collectFileSources(node: SparkPlan): Unit = {
//      node match {
//        case scan: FileSourceScanExec if isParquetOrOrc(scan.relation.fileFormat) =>
//          debug(s"Found FileSourceScanExec: ${scan.relation.location.rootPaths}")
//
//          val path = scan.relation.location.rootPaths.headOption.map(_.toString).getOrElse("unknown")
//          val tableName = extractTableNameFromPath(path)
//          val format = getFileSourceType(scan.relation.fileFormat)
//          val columns = scan.output.map(_.name).toList
//          val dataFilters = scan.dataFilters.map(_.sql).toList
//          val partitionFilters = scan.partitionFilters.map(_.sql).toList
//          val allFilters = dataFilters ++ partitionFilters
//
//          val metrics = aggregateStageMetrics(stageMetrics)
//
//          // Calculate initial metrics from scan info
//          val selectedPartitions = scan.selectedPartitions
//          val files = selectedPartitions.flatMap(_.files)
//          val initialBytes = files.map(_.getLen).sum
//          val initialFiles = files.length.toLong
//
//          sources += SourceNodeDetail(
//            tableName = tableName,
//            folderName = path,
//            sourceType = format,
//            columnsProjected = columns,
//            filtersApplied = allFilters,
//            metricsBeforeFilter = MetricsDetail(
//              recordsCount = if (metrics.inputRecords > 0) metrics.inputRecords else 0L,
//              bytesProcessed = if (initialBytes > 0) initialBytes else metrics.inputBytes,
//              filesCount = if (initialFiles > 0) initialFiles else metrics.filesRead
//            ),
//            metricsAfterFilter = MetricsDetail(
//              recordsCount = metrics.outputRecords,
//              bytesProcessed = metrics.outputBytes,
//              filesCount = metrics.filesRead
//            ),
//            partitionsScanned = selectedPartitions.take(5).map(_.toString).toList
//
//          )
//
//        case _ =>
//        // Continue searching in children
//      }
//
//      // Recursively process children
//      node.children.foreach(collectFileSources)
//    }
//
//    collectFileSources(plan)
//
//    debug(s"Found ${sources.size} source nodes")
//    sources.toList
//  }
//
//  private def extractTargetNode(
//                                 logical: LogicalPlan,
//                                 physical: SparkPlan,
//                                 stageMetrics: mutable.Map[Int, StageMetrics]
//                               ): Option[TargetNodeDetail] = {
//    val metrics = aggregateStageMetrics(stageMetrics)
//
//    debug("Searching for target node")
//
//    // Check physical plan first
//    physical.collectFirst {
//      case DataWritingCommandExec(cmd, _) => cmd
//    }.flatMap {
//      case insert: InsertIntoHadoopFsRelationCommand =>
//        debug(s"Found InsertIntoHadoopFsRelationCommand: ${insert.outputPath}")
//        Some(TargetNodeDetail(
//          tableName = extractTableNameFromPath(insert.outputPath.toString),
//          folderName = insert.outputPath.toString,
//          targetType = getFileSourceType(insert.fileFormat),
//          metrics = MetricsDetail(
//            recordsCount = metrics.outputRecords,
//            bytesProcessed = metrics.outputBytes,
//            filesCount = if (metrics.filesWritten > 0) metrics.filesWritten else 1L
//          )
//        ))
//      case _ => None
//    }.orElse {
//      // Check logical plan
//      logical.collectFirst {
//        case save: SaveIntoDataSourceCommand =>
//          val path = save.options.getOrElse("path", "unknown")
//          debug(s"Found SaveIntoDataSourceCommand: $path")
//          TargetNodeDetail(
//            tableName = extractTableNameFromPath(path),
//            folderName = path,
//            targetType = "PARQUET_FILE",
//            metrics = MetricsDetail(
//              recordsCount = metrics.outputRecords,
//              bytesProcessed = metrics.outputBytes,
//              filesCount = if (metrics.filesWritten > 0) metrics.filesWritten else 1L
//            )
//          )
//        case create: CreateDataSourceTableAsSelectCommand =>
//          val path = create.table.storage.locationUri.map(_.toString).getOrElse("unknown")
//          debug(s"Found CreateDataSourceTableAsSelectCommand: $path")
//          TargetNodeDetail(
//            tableName = create.table.identifier.table,
//            folderName = path,
//            targetType = create.table.provider.getOrElse("UNKNOWN"),
//            metrics = MetricsDetail(
//              recordsCount = metrics.outputRecords,
//              bytesProcessed = metrics.outputBytes,
//              filesCount = if (metrics.filesWritten > 0) metrics.filesWritten else 1L
//            )
//          )
//      }
//    }
//  }
//
//  private def createBasicWriteMetadata(
//                                        jobId: Int,
//                                        stageMetrics: mutable.Map[Int, StageMetrics],
//                                        executionTime: Long
//                                      ): Unit = {
//    val metrics = aggregateStageMetrics(stageMetrics)
//
//    val metadata = ActionMetadata(
//      actionId = s"job_${jobId}_${System.currentTimeMillis()}",
//      actionType = "WRITE_DETECTED",
//      timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
//      sourceNodes = List.empty,
//      targetNode = Some(TargetNodeDetail(
//        tableName = "detected_output",
//        folderName = "/detected/output",
//        targetType = "FILE_SINK",
//        metrics = MetricsDetail(
//          recordsCount = metrics.outputRecords,
//          bytesProcessed = metrics.outputBytes,
//          filesCount = metrics.filesWritten
//        )
//      )),
//      executionTimeMs = executionTime,
//      sparkJobId = jobId,
//      sparkStageIds = stageMetrics.keys.toList
//    )
//
//    metadataCallback(metadata)
//  }
//
//  // Helper methods
//  private def isParquetOrOrc(fileFormat: FileFormat): Boolean = {
//    fileFormat match {
//      case _: ParquetFileFormat => true
//      case _: OrcFileFormat => true
//      case _ => false
//    }
//  }
//
//  private def getFileSourceType(fileFormat: FileFormat): String = {
//    fileFormat match {
//      case _: ParquetFileFormat => "PARQUET_FILE"
//      case _: OrcFileFormat => "ORC_FILE"
//      case _ => "UNKNOWN_FILE"
//    }
//  }
//
//  private def extractTableNameFromPath(path: String): String = {
//    val p = new Path(path)
//    p.getName.replaceAll("\\.(parquet|orc)$", "")
//  }
//
//  private def aggregateStageMetrics(stageMetrics: mutable.Map[Int, StageMetrics]): StageMetrics = {
//    stageMetrics.values.foldLeft(StageMetrics(-1)) { (acc, m) =>
//      acc.inputRecords += m.inputRecords
//      acc.inputBytes += m.inputBytes
//      acc.outputRecords += m.outputRecords
//      acc.outputBytes += m.outputBytes
//      acc.filesRead += m.filesRead
//      acc.filesWritten += m.filesWritten
//      acc
//    }
//  }
//
//  private def determineActionType(plan: LogicalPlan): String = {
//    plan match {
//      case _: Command => "WRITE_COMMAND"
//      case _ if plan.find(_.isInstanceOf[InsertIntoStatement]).isDefined => "INSERT"
//      case _ if plan.find(_.isInstanceOf[CreateTableAsSelect]).isDefined => "CREATE_TABLE_AS_SELECT"
//      case _ => "TRANSFORMATION"
//    }
//  }
//
//  private def formatBytes(bytes: Long): String = {
//    if (bytes <= 0) "0 B"
//    else if (bytes < 1024) s"$bytes B"
//    else if (bytes < 1024 * 1024) f"${bytes / 1024.0}%.2f KB"
//    else if (bytes < 1024 * 1024 * 1024) f"${bytes / (1024.0 * 1024)}%.2f MB"
//    else f"${bytes / (1024.0 * 1024 * 1024)}%.2f GB"
//  }
//}