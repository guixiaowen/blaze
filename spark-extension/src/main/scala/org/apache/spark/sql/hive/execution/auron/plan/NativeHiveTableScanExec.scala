/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive.execution.auron.plan

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.{JniBridge, MetricNode, NativeConverters, NativeHelper, NativeRDD, Shims}
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalogUtils, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.types.{NullType, StructField, StructType}
import org.apache.auron.{sparkver, protobuf => pb}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.spark.sql.auron.util.HiveTableUtil
import org.apache.spark.sql.hive.client.HiveClientImpl

import java.util.UUID

case class NativeHiveTableScanExec(basedHiveScan: HiveTableScanExec)
  extends NativeHiveTableScanBase(basedHiveScan)
    with Logging {

  private lazy val table: HiveTable = HiveClientImpl.toHiveTable(relation.tableMeta)
  private lazy val fileFormat = HiveTableUtil.getFileFormat(table.getInputFormatClass)

  override def doExecuteNative(): NativeRDD = {
    val nativeMetrics = MetricNode(
      metrics,
      Nil,
      Some({
        case ("bytes_scanned", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incBytesRead(v)
        case ("output_rows", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incRecordsRead(v)
        case _ =>
      }))
    // val nativePruningPredicateFilters = this.nativePruningPredicateFilters
    val nativeFileSchema = this.nativeFileSchema
    val nativeFileGroups = this.nativeFileGroups
    val nativePartitionSchema = this.nativePartitionSchema

    val projection = schema.map(field => relation.schema.fieldIndex(field.name))
    val broadcastedHadoopConf = this.broadcastedHadoopConf
    val numPartitions = partitions.length

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      None,
      Nil,
      rddShuffleReadFull = true,
      (partition, _) => {
        val resourceId = s"NativeHiveTableScan:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[FilePartition])
        val nativeFileScanConf = pb.FileScanExecConf
          .newBuilder()
          .setNumPartitions(numPartitions)
          .setPartitionIndex(partition.index)
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .setFileGroup(nativeFileGroup)
          .addAllProjection(projection.map(Integer.valueOf).asJava)
          .setPartitionSchema(nativePartitionSchema)
          .build()
        if (fileFormat.equals("orc")) {
          val nativeOrcScanExecBuilder = pb.OrcScanExecNode
            .newBuilder()
            .setBaseConf(nativeFileScanConf)
            .setFsResourceId(resourceId)
            .addAllPruningPredicates(new java.util.ArrayList()) // not support this filter

          pb.PhysicalPlanNode
            .newBuilder()
            .setOrcScan(nativeOrcScanExecBuilder.build())
            .build()
        } else if (fileFormat.equals("parquet")) {
          val nativeParquetScanExecBuilder = pb.ParquetScanExecNode
            .newBuilder()
            .setBaseConf(nativeFileScanConf)
            .setFsResourceId(resourceId)
            .addAllPruningPredicates(new java.util.ArrayList()) // not support this filter

          pb.PhysicalPlanNode
            .newBuilder()
            .setParquetScan(nativeParquetScanExecBuilder.build())
            .build()
        }
      },
      friendlyName = "NativeRDD.PaimonScan")
  }

  override val nodeName: String =
    s"NativeHiveTableScan $tableName"

  override def getFilePartitions(): Array[FilePartition] = {

    if (relation.isPartitioned) {
      // partitioned table

    } else {
      // no partitioned table
      val tablePath = table.getPath


    }
    Array[FilePartition]
  }


}
