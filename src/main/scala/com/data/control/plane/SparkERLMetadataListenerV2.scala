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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

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

// Enhanced case classes for better tracking
case class StageInfo(
                      stageId: Int,
                      stageName: String,
                      rddName: String,
                      details: String,
                      stageType: StageType.Value = StageType.UNKNOWN
                    )

case class SourceStageMapping(
                               sourcePath: String,
                               tableName: String,
                               readStageId: Int,
                               processStageIds: List[Int] = List.empty
                             )

object StageType extends Enumeration {
  val READ, PROCESS, WRITE, SHUFFLE, UNKNOWN = Value
}

class SparkETLMetadataListenerV2 extends SparkListener with QueryExecutionListener {

  // Thread-safe collections
  private val executionIdToQueryExecution = new ConcurrentHashMap[Long, QueryExecution]()
  private val jobToExecutionId = new ConcurrentHashMap[Int, Long]()
  private val jobToStageMetrics = new ConcurrentHashMap[Int, mutable.Map[Int, StageMetrics]]()
  private val jobStartTimes = new ConcurrentHashMap[Int, Long]()
  private val recentQueryExecutions = new ConcurrentHashMap[Long, QueryExecution]()

  // Enhanced tracking
  private val stageIdToInfo = new ConcurrentHashMap[Int, StageInfo]()
  private val jobToSourceMappings = new ConcurrentHashMap[Int, List[SourceStageMapping]]()
  private val stageIdToSourcePath = new ConcurrentHashMap[Int, String]()

  // Enable debug mode
  private var debugMode = true

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
                           var filesWritten: Long = 0L,
                           var stageType: StageType.Value = StageType.UNKNOWN
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

    debug(s"Job $jobId started with ${jobStart.stageIds.length} stages: ${jobStart.stageIds.mkString(", ")}")

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

    // Determine stage type
    val stageType = determineStageType(stageInfo)

    // Store stage information
    val info = StageInfo(
      stageId = stageId,
      stageName = stageInfo.name,
      rddName = stageInfo.rddInfos.headOption.map(_.name).getOrElse(""),
      details = stageInfo.details,
      stageType = stageType
    )
    stageIdToInfo.put(stageId, info)

    debug(s"Stage $stageId submitted: ${stageInfo.name} [Type: $stageType]")

    // Extract source path for READ stages
    if (stageType == StageType.READ) {
      extractSourcePathFromStage(stageInfo).foreach { path =>
        stageIdToSourcePath.put(stageId, path)
        debug(s"Stage $stageId (READ) associated with path: $path")
      }
    }
  }

  private def determineStageType(stageInfo: StageInfo): StageType.Value = {
    val name = stageInfo.stageName.toLowerCase
    val rddName = stageInfo.rddName.toLowerCase
    val details = stageInfo.details.toLowerCase

    if (name.contains("scan") || name.contains("filesource") || rddName.contains("filesource")) {
      StageType.READ
    } else if (name.contains("write") || name.contains("insert") || rddName.contains("write")) {
      StageType.WRITE
    } else if (name.contains("exchange") || name.contains("shuffle")) {
      StageType.SHUFFLE
    } else if (name.contains("filter") || name.contains("project") || name.contains("hashjoin") ||
      name.contains("aggregate") || name.contains("sort")) {
      StageType.PROCESS
    } else {
      StageType.UNKNOWN
    }
  }

  private def extractSourcePathFromStage(stageInfo: StageInfo): Option[String] = {
    val allText = s"${stageInfo.stageName} ${stageInfo.rddName} ${stageInfo.details}"
    val pathPattern = """.*?([/\w\-\.]+\.(?:parquet|orc)).*""".r

    pathPattern.findFirstMatchIn(allText).map(_.group(1))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageId = stageInfo.stageId
    val storedInfo = stageIdToInfo.get(stageId)

    debug(s"Stage $stageId completed: ${stageInfo.name}")

    if (debugMode) {
      println(s"[DEBUG] Stage $stageId accumulables:")
      stageInfo.accumulables.foreach { case (id, accum) =>
        println(s"  - ${accum.name.getOrElse("unnamed")}: ${accum.value.getOrElse("null")}")
      }

      if (stageInfo.taskMetrics != null) {
        val tm = stageInfo.taskMetrics
        println(s"[DEBUG] Stage $stageId task metrics:")
        println(s"  - Input records: ${tm.inputMetrics.recordsRead}")
        println(s"  - Input bytes: ${tm.inputMetrics.bytesRead}")
        println(s"  - Output records: ${tm.outputMetrics.recordsWritten}")
        println(s"  - Output bytes: ${tm.outputMetrics.bytesWritten}")
      }
    }

    // Extract metrics from this specific stage
    val metrics = StageMetrics(stageId)
    if (storedInfo != null) {
      metrics.stageType = storedInfo.stageType
    }

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
            metrics.outputRecords = math.max(metrics.outputRecords, value)
          case n if n.contains("number of files read") =>
            metrics.filesRead = math.max(metrics.filesRead, value)
          case n if n.contains("bytes read") || n.contains("input size") || n.contains("size of files read") =>
            metrics.inputBytes = math.max(metrics.inputBytes, value)
          case n if n.contains("records read") || n.contains("number of input rows") =>
            metrics.inputRecords = math.max(metrics.inputRecords, value)
          case n if n.contains("number of written files") =>
            metrics.filesWritten = math.max(metrics.filesWritten, value)
          case n if n.contains("written output rows") =>
            metrics.outputRecords = math.max(metrics.outputRecords, value)
          case n if n.contains("bytes written") =>
            metrics.outputBytes = math.max(metrics.outputBytes, value)
          case _ =>
        }
      }
    }

    // Also check task metrics as fallback
    if (stageInfo.taskMetrics != null) {
      val tm = stageInfo.taskMetrics
      if (metrics.inputRecords == 0) metrics.inputRecords = tm.inputMetrics.recordsRead
      if (metrics.inputBytes == 0) metrics.inputBytes = tm.inputMetrics.bytesRead
      if (metrics.outputRecords == 0) metrics.outputRecords = tm.outputMetrics.recordsWritten
      if (metrics.outputBytes == 0) metrics.outputBytes = tm.outputMetrics.bytesWritten
    }

    debug(s"Stage $stageId [${metrics.stageType}] final metrics: " +
      s"input=${metrics.inputRecords}/${formatBytes(metrics.inputBytes)}, " +
      s"output=${metrics.outputRecords}/${formatBytes(metrics.outputBytes)}, " +
      s"files_read=${metrics.filesRead}, files_written=${metrics.filesWritten}")

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
      debug(s"Found QueryExecution for job $jobId, extracting metadata...")

      // Build source mappings first
      buildSourceStageMappings(jobId, qe.get, stageMetrics.get)

      extractAndReportMetadata(jobId, qe.get, stageMetrics.get, executionTime)
    }

    // Cleanup
    jobStartTimes.remove(jobId)
    jobToExecutionId.remove(jobId)
    jobToStageMetrics.remove(jobId)
    jobToSourceMappings.remove(jobId)
    executionId.foreach(executionIdToQueryExecution.remove)

    // Clean up stage info for this job's stages
    stageMetrics.foreach { metrics =>
      metrics.keys.foreach { stageId =>
        stageIdToInfo.remove(stageId)
        stageIdToSourcePath.remove(stageId)
      }
    }
  }

  private def buildSourceStageMappings(
                                        jobId: Int,
                                        qe: QueryExecution,
                                        stageMetrics: mutable.Map[Int, StageMetrics]
                                      ): Unit = {
    debug("Building source-to-stage mappings...")

    val mappings = mutable.ListBuffer[SourceStageMapping]()
    val readStages = stageMetrics.filter(_._2.stageType == StageType.READ).toSeq.sortBy(_._1)

    debug(s"Found ${readStages.size} READ stages: ${readStages.map(_._1).mkString(", ")}")

    // Extract file sources from the execution plan
    val fileSources = extractFileSourcesFromPlan(qe.executedPlan)
    debug(s"Found ${fileSources.size} file sources in plan")

    // Map each file source to its corresponding read stage
    fileSources.zipWithIndex.foreach { case (source, index) =>
      if (index < readStages.size) {
        val readStageId = readStages(index)._1
        val sourcePath = source.relation.location.rootPaths.headOption.map(_.toString).getOrElse("unknown")
        val tableName = extractTableNameFromPath(sourcePath)

        // Find process stages that might be related to this source
        val processStages = findProcessStagesForSource(readStageId, stageMetrics)

        val mapping = SourceStageMapping(
          sourcePath = sourcePath,
          tableName = tableName,
          readStageId = readStageId,
          processStageIds = processStages
        )

        mappings += mapping
        debug(s"Mapped source '$tableName' to read stage $readStageId with process stages: ${processStages.mkString(", ")}")
      }
    }

    jobToSourceMappings.put(jobId, mappings.toList)
  }

  private def extractFileSourcesFromPlan(plan: SparkPlan): List[FileSourceScanExec] = {
    val sources = mutable.ListBuffer[FileSourceScanExec]()

    def collectSources(node: SparkPlan): Unit = {
      node match {
        case scan: FileSourceScanExec if isParquetOrOrc(scan.relation.fileFormat) =>
          sources += scan
        case _ =>
      }
      node.children.foreach(collectSources)
    }

    collectSources(plan)
    sources.toList
  }

  private def findProcessStagesForSource(readStageId: Int, stageMetrics: mutable.Map[Int, StageMetrics]): List[Int] = {
    // For now, return all process stages that come after the read stage
    // This is a simplified approach - in practice, you might want more sophisticated stage dependency tracking
    stageMetrics.filter { case (stageId, metrics) =>
      stageId > readStageId && metrics.stageType == StageType.PROCESS
    }.keys.toList.sorted
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

      // Extract sources with proper metrics mapping using the source mappings
      val sources = extractSourceNodesWithCorrectMetrics(qe.executedPlan, stageMetrics, jobId)
      val target = extractTargetNodeWithMetrics(qe.logical, qe.executedPlan, stageMetrics)

      debug(s"Found ${sources.size} source nodes and ${if (target.isDefined) "1" else "0"} target node")

      // Only report if we have sources (target is optional for some operations)
      if (sources.nonEmpty) {
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

  private def extractSourceNodesWithCorrectMetrics(
                                                    plan: SparkPlan,
                                                    stageMetrics: mutable.Map[Int, StageMetrics],
                                                    jobId: Int
                                                  ): List[SourceNodeDetail] = {
    val sources = mutable.ListBuffer[SourceNodeDetail]()
    val sourceMappings = Option(jobToSourceMappings.get(jobId)).getOrElse(List.empty)

    debug("Extracting source nodes with corrected metrics")

    def collectFileSources(node: SparkPlan): Unit = {
      node match {
        case scan: FileSourceScanExec if isParquetOrOrc(scan.relation.fileFormat) =>
          val path = scan.relation.location.rootPaths.headOption.map(_.toString).getOrElse("unknown")
          val tableName = extractTableNameFromPath(path)
          val format = getFileSourceType(scan.relation.fileFormat)
          val columns = scan.output.map(_.name).toList
          val dataFilters = scan.dataFilters.map(_.sql).toList
          val partitionFilters = scan.partitionFilters.map(_.sql).toList
          val allFilters = dataFilters ++ partitionFilters

          // Find the corresponding source mapping
          val sourceMapping = sourceMappings.find(m =>
            m.tableName == tableName || m.sourcePath.contains(tableName) || tableName.contains(m.sourcePath)
          )

          val (beforeMetrics, afterMetrics) = sourceMapping match {
            case Some(mapping) =>
              val readStage = stageMetrics.get(mapping.readStageId)

              // Before filter: Use the raw scan information + read stage input metrics
              val selectedPartitions = scan.selectedPartitions
              val files = selectedPartitions.flatMap(_.files)
              val initialBytes = files.map(_.getLen).sum
              val initialFiles = files.length.toLong

              val beforeRecords = readStage.map(_.inputRecords).getOrElse(0L)
              val beforeBytes = if (initialBytes > 0) initialBytes else readStage.map(_.inputBytes).getOrElse(0L)
              val beforeFiles = if (initialFiles > 0) initialFiles else readStage.map(_.filesRead).getOrElse(0L)

              // After filter: Use the output of read stage (which includes filtering)
              val afterRecords = readStage.map(_.outputRecords).getOrElse(beforeRecords)
              val afterBytes = readStage.map(_.outputBytes).getOrElse(0L)

              debug(s"Source $tableName metrics: Before[${beforeRecords}r/${formatBytes(beforeBytes)}] -> After[${afterRecords}r/${formatBytes(afterBytes)}]")

              val before = MetricsDetail(
                recordsCount = beforeRecords,
                bytesProcessed = beforeBytes,
                filesCount = beforeFiles
              )

              val after = MetricsDetail(
                recordsCount = afterRecords,
                bytesProcessed = if (afterBytes > 0) afterBytes else beforeBytes, // Fallback to before if no after bytes
                filesCount = beforeFiles
              )

              (before, after)

            case None =>
              // Fallback logic if mapping is not found
              debug(s"No mapping found for source $tableName, using fallback logic")

              val selectedPartitions = scan.selectedPartitions
              val files = selectedPartitions.flatMap(_.files)
              val initialBytes = files.map(_.getLen).sum
              val initialFiles = files.length.toLong

              val fallbackBefore = MetricsDetail(
                recordsCount = 0L, // We don't have reliable record count without stage mapping
                bytesProcessed = initialBytes,
                filesCount = initialFiles
              )

              (fallbackBefore, fallbackBefore)
          }

          sources += SourceNodeDetail(
            tableName = tableName,
            folderName = path,
            sourceType = format,
            columnsProjected = columns,
            filtersApplied = allFilters,
            metricsBeforeFilter = beforeMetrics,
            metricsAfterFilter = afterMetrics,
            partitionsScanned = scan.selectedPartitions.take(5).map(_.toString).toList
          )

        case _ =>
      }

      // Recursively process children
      node.children.foreach(collectFileSources)
    }

    collectFileSources(plan)
    sources.toList
  }

  private def extractTargetNodeWithMetrics(
                                            logical: LogicalPlan,
                                            physical: SparkPlan,
                                            stageMetrics: mutable.Map[Int, StageMetrics]
                                          ): Option[TargetNodeDetail] = {
    // For target, we want the write stage metrics
    val writeStageMetrics = stageMetrics.values
      .filter(m => m.stageType == StageType.WRITE || m.outputRecords > 0 || m.filesWritten > 0)
      .toSeq
      .sortBy(_.stageId)
      .lastOption
      .getOrElse {
        // Fallback: find any stage with output metrics
        stageMetrics.values.filter(_.outputRecords > 0).toSeq.sortBy(_.stageId).lastOption
          .getOrElse(StageMetrics(-1))
      }

    debug(s"Target metrics from stage ${writeStageMetrics.stageId}: ${writeStageMetrics.outputRecords} records, ${formatBytes(writeStageMetrics.outputBytes)}")

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
      }
    }
  }

  // Helper methods
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
    val metadataListener = new SparkETLMetadataListenerV2()
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
