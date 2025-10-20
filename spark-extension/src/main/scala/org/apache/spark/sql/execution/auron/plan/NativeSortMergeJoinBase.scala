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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.JoinOn
import org.apache.auron.protobuf.PhysicalPlanNode
import org.apache.auron.protobuf.SortMergeJoinExecNode
import org.apache.auron.protobuf.SortOptions

abstract class NativeSortMergeJoinBase(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType)
    extends BinaryExecNode
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

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  private def nativeSchema = Util.getNativeSchema(output)

  private def nativeJoinOn = leftKeys.zip(rightKeys).map { case (leftKey, rightKey) =>
    val leftKeyExpr = NativeConverters.convertExpr(leftKey)
    val rightKeyExpr = NativeConverters.convertExpr(rightKey)
    JoinOn
      .newBuilder()
      .setLeft(leftKeyExpr)
      .setRight(rightKeyExpr)
      .build()
  }

  private def nativeSortOptions = nativeJoinOn.map(_ => {
    SortOptions
      .newBuilder()
      .setAsc(true)
      .setNullsFirst(true)
      .build()
  })

  private def nativeJoinType = NativeConverters.convertJoinType(joinType)

  // check whether native converting is supported
  nativeSchema
  nativeSortOptions
  nativeJoinOn
  nativeJoinType

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    val nativeMetrics = SparkMetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val nativeSortOptions = this.nativeSortOptions
    val nativeJoinOn = this.nativeJoinOn
    val nativeJoinType = this.nativeJoinType

    val (partitions, partitioner) = if (joinType != RightOuter) {
      (leftRDD.partitions, leftRDD.partitioner)
    } else {
      (rightRDD.partitions, rightRDD.partitioner)
    }
    val dependencies = Seq(new OneToOneDependency(leftRDD), new OneToOneDependency(rightRDD))
    val isShuffleReadFull = joinType match {
      case _: InnerLike =>
        logInfo("SortMergeJoin Inner mark shuffleReadFull = false")
        false
      case LeftAnti | LeftSemi =>
        logInfo("SortMergeJoin LeftAnti|LeftSemi mark shuffleReadFull = false")
        false
      case _: ExistenceJoin =>
        logInfo("SortMergeJoin ExistenceJoin mark shuffleReadFull = false")
        false
      case _ => leftRDD.isShuffleReadFull && rightRDD.isShuffleReadFull
    }

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      partitioner,
      dependencies,
      isShuffleReadFull,
      (partition, taskContext) => {
        val leftPartition = leftRDD.partitions(partition.index)
        val leftChild = leftRDD.nativePlan(leftPartition, taskContext)

        val rightPartition = rightRDD.partitions(partition.index)
        val rightChild = rightRDD.nativePlan(rightPartition, taskContext)

        val sortMergeJoinExec = SortMergeJoinExecNode
          .newBuilder()
          .setSchema(nativeSchema)
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)
          .addAllOn(nativeJoinOn.asJava)
          .addAllSortOptions(nativeSortOptions.asJava)
        PhysicalPlanNode.newBuilder().setSortMergeJoin(sortMergeJoinExec).build()
      },
      friendlyName = "NativeRDD.SortMergeJoin")
  }
}
