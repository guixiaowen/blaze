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
package org.apache.spark.sql.execution.auron.plan

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.OrderedDistribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.PhysicalExprNode
import org.apache.auron.protobuf.PhysicalPlanNode
import org.apache.auron.protobuf.PhysicalSortExprNode
import org.apache.auron.protobuf.SortExecNode

abstract class NativeSortBase(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set(
        "stage_id",
        "output_rows",
        "elapsed_compute",
        "mem_spill_count",
        "mem_spill_size",
        "mem_spill_iotime",
        "disk_spill_size",
        "disk_spill_iotime",
        "input_batch_count",
        "input_batch_mem_size",
        "input_row_count"))
      .toSeq: _*)

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) {
      OrderedDistribution(sortOrder) :: Nil
    } else {
      UnspecifiedDistribution :: Nil
    }

  private def nativeSortExprs = sortOrder.map { sortOrder =>
    PhysicalExprNode
      .newBuilder()
      .setSort(
        PhysicalSortExprNode
          .newBuilder()
          .setExpr(NativeConverters.convertExpr(sortOrder.child))
          .setAsc(sortOrder.direction == Ascending)
          .setNullsFirst(sortOrder.nullOrdering == NullsFirst)
          .build())
      .build()
  }

  // check whether native converting is supported
  nativeSortExprs

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = SparkMetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeSortExprs = this.nativeSortExprs

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddPartitioner = inputRDD.partitioner,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeSortExec = SortExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllExpr(nativeSortExprs.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setSort(nativeSortExec).build()
      },
      friendlyName = "NativeRDD.Sort")
  }
}
