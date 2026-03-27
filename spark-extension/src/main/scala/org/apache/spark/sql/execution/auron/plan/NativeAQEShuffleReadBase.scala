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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.auron.{NativeRDD, NativeSupports}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan, UnaryExecNode}

abstract class NativeAQEShuffleReadBase(override val child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec])
  extends UnaryExecNode
  with NativeSupports {

  private def shuffleStage = child match {
    case stage: ShuffleQueryStageExec => Some(stage)
    case _ => None
  }

  private lazy val shuffleRDD: RDD[_] = {
    shuffleStage match {
      case Some(stage) =>
        sendDriverMetrics()
        stage.shuffle.getShuffleRDD(partitionSpecs.toArray)
      case _ =>
        throw new IllegalStateException("operating on canonicalized plan")
    }
  }

  override protected def doExecuteNative(): NativeRDD = ???

  override def output: Seq[Attribute] = child.output

}
