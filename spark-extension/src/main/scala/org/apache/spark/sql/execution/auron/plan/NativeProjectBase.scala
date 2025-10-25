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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.analysis.ResolvedStar
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.auron.plan.NativeProjectBase.getNativeProjectBuilder
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.ArrowType
import org.apache.auron.protobuf.PhysicalExprNode
import org.apache.auron.protobuf.PhysicalPlanNode
import org.apache.auron.protobuf.ProjectionExecNode

abstract class NativeProjectBase(projectList: Seq[NamedExpression], override val child: SparkPlan)
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

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  private def nativeProject = getNativeProjectBuilder(projectList).buildPartial()

  // check whether native converting is supported
  nativeProject

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = SparkMetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeProject = this.nativeProject

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddPartitioner = inputRDD.partitioner,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeProjectExec = nativeProject.toBuilder
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .build()
        PhysicalPlanNode.newBuilder().setProjection(nativeProjectExec).build()
      },
      friendlyName = "NativeRDD.Project")
  }
}

object NativeProjectBase {
  def getNativeProjectBuilder(projectList: Seq[NamedExpression]): ProjectionExecNode.Builder = {
    val nativeDataTypes = ArrayBuffer[ArrowType]()
    val nativeNamedExprs = ArrayBuffer[(String, PhysicalExprNode)]()
    var numAddedColumns = 0

    projectList.foreach { projectExpr =>
      def addNamedExpression(namedExpression: NamedExpression): Unit = {
        namedExpression match {
          case star: ResolvedStar =>
            for (expr <- star.expressions) {
              addNamedExpression(expr)
            }

          case alias: Alias =>
            nativeNamedExprs.append(
              (Util.getFieldNameByExprId(alias), NativeConverters.convertExpr(alias.child)))
            nativeDataTypes.append(NativeConverters.convertDataType(alias.dataType))
            numAddedColumns += 1

          case named =>
            nativeNamedExprs.append(
              (Util.getFieldNameByExprId(named), NativeConverters.convertExpr(named)))
            nativeDataTypes.append(NativeConverters.convertDataType(named.dataType))
            numAddedColumns += 1
        }
      }
      addNamedExpression(projectExpr)
    }

    ProjectionExecNode
      .newBuilder()
      .addAllExprName(nativeNamedExprs.map(_._1).asJava)
      .addAllExpr(nativeNamedExprs.map(_._2).asJava)
      .addAllDataType(nativeDataTypes.asJava)
  }
}
