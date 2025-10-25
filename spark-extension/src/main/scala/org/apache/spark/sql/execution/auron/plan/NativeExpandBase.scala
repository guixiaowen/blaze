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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.ExpandExecNode
import org.apache.auron.protobuf.ExpandProjection
import org.apache.auron.protobuf.PhysicalPlanNode

abstract class NativeExpandBase(
    projections: Seq[Seq[Expression]],
    override val output: Seq[Attribute],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(
        Set(
          "stage_id",
          "output_rows",
          "elapsed_compute",
          "input_batch_count",
          "input_batch_mem_size",
          "input_row_count"))
      .toSeq: _*)

  override def outputPartitioning: Partitioning = UnknownPartitioning(0)
  override def outputOrdering: Seq[SortOrder] = Nil

  private def nativeSchema = Util.getNativeSchema(output)
  private def nativeProjections = projections.map { projection =>
    projection
      .zip(Util.getSchema(output).fields.map(_.dataType))
      .map(e => NativeConverters.convertExpr(Cast(e._1, e._2)))
  }

  // check whether native converting is supported
  nativeSchema
  nativeProjections

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = SparkMetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeSchema = this.nativeSchema
    val nativeProjections = this.nativeProjections

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddPartitioner = inputRDD.partitioner,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeExpandExec = ExpandExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .setSchema(nativeSchema)
          .addAllProjections(nativeProjections.map { projection =>
            ExpandProjection
              .newBuilder()
              .addAllExpr(projection.asJava)
              .build()
          }.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setExpand(nativeExpandExec).build()
      },
      friendlyName = "NativeRDD.Expand")
  }
}
