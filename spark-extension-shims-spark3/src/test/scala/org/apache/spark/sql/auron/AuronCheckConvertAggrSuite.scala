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
package org.apache.spark.sql.auron

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.auron.plan.NativeAggExec
import org.apache.spark.sql.test.SharedSparkSession

class AuronCheckConvertAggrSuite
    extends QueryTest
    with SharedSparkSession
    with AuronSQLTestHelper
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {
  import testImplicits._

  test("test hash aggr convert to native") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkSeparateAggr")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()
    Seq((1, 2, "test test"))
      .toDF("c1", "c2", "part")
      .createOrReplaceTempView("separate_hash_aggr")
    val executePlan = spark.sql("select c1, count(1) from separate_hash_aggr group by c1")
    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val hashAggregateExec =
      plan.executedPlan.collectFirst {
        case hashAggregateExec: HashAggregateExec
            if hashAggregateExec.aggregateExpressions.forall(_.mode == Partial) =>
          hashAggregateExec
      }
    val afterConvertPlan = AuronConverters.convertSparkPlan(hashAggregateExec.get)
    assert(afterConvertPlan.isInstanceOf[NativeAggExec])
    checkAnswer(executePlan, Seq(Row(1, 1)))
  }

  test("test hash aggr do not convert to native") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkSeparateAggr")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.auron.enable.hash.aggr", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()
    Seq((1, 2, "test test"))
      .toDF("c1", "c2", "part")
      .createOrReplaceTempView("separate_hash_aggr")
    val executePlan = spark.sql("select c1, count(1) from separate_hash_aggr group by c1")
    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val hashAggregateExec =
      plan.executedPlan
        .collectFirst {
          case hashAggregateExec: HashAggregateExec
              if hashAggregateExec.aggregateExpressions.forall(_.mode == Partial) =>
            hashAggregateExec
        }
    val afterConvertPlan = AuronConverters.convertSparkPlan(hashAggregateExec.get)
    assert(afterConvertPlan.isInstanceOf[HashAggregateExec])
    checkAnswer(executePlan, Seq(Row(1, 1)))
  }

  test("test sort aggr convert to native") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkSeparateAggr")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.sql.execution.useObjectHashAggregateExec", "false")
      .config("spark.sql.execution.useHashAggregateExec", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()
    Seq((1, 2, "test test"))
      .toDF("c1", "c2", "part")
      .createOrReplaceTempView("separate_hash_aggr")
    val executePlan = spark.sql("select c1, collect_list(c2) from separate_hash_aggr group by c1")
    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val sortAggregateExec =
      plan.executedPlan
        .collectFirst {
          case sortAggregateExec: SortAggregateExec
              if sortAggregateExec.aggregateExpressions.forall(_.mode == Partial) =>
            sortAggregateExec
        }
    val afterConvertPlan = AuronConverters.convertSparkPlan(sortAggregateExec.get)
    assert(afterConvertPlan.isInstanceOf[NativeAggExec])
    checkAnswer(executePlan, Seq(Row(1, ArrayBuffer(2))))
  }

  test("test sort aggr do not convert to native") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkSeparateAggr")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.sql.execution.useObjectHashAggregateExec", "false")
      .config("spark.sql.execution.useHashAggregateExec", "false")
      .config("spark.auron.enable.sort.aggr", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()
    Seq((1, 2, "test test"))
      .toDF("c1", "c2", "part")
      .createOrReplaceTempView("separate_hash_aggr")
    val executePlan = spark.sql("select c1, collect_list(c2) from separate_hash_aggr group by c1")
    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val sortAggregateExec =
      plan.executedPlan
        .collectFirst {
          case sortAggregateExec: SortAggregateExec
              if sortAggregateExec.aggregateExpressions.forall(_.mode == Partial) =>
            sortAggregateExec
        }
    val afterConvertPlan = AuronConverters.convertSparkPlan(sortAggregateExec.get)
    assert(afterConvertPlan.isInstanceOf[SortAggregateExec])
    checkAnswer(executePlan, Seq(Row(1, ArrayBuffer(2))))
  }

  test("test object hash aggr convert to native") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkSeparateAggr")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.sql.execution.useHashAggregateExec", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()
    Seq((1, 2, "test test"))
      .toDF("c1", "c2", "part")
      .createOrReplaceTempView("separate_hash_aggr")
    val executePlan = spark.sql("select c1, collect_list(c2) from separate_hash_aggr group by c1")
    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val objectHashAggregateExec =
      plan.executedPlan
        .collectFirst {
          case objectHashAggregateExec: ObjectHashAggregateExec
              if objectHashAggregateExec.aggregateExpressions.forall(_.mode == Partial) =>
            objectHashAggregateExec
        }
    val afterConvertPlan = AuronConverters.convertSparkPlan(objectHashAggregateExec.get)
    assert(afterConvertPlan.isInstanceOf[NativeAggExec])
    checkAnswer(executePlan, Seq(Row(1, ArrayBuffer(2))))
  }

  test("test object hash aggr do not convert to native") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkSeparateAggr")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.sql.execution.useHashAggregateExec", "false")
      .config("spark.auron.enable.object.hash.aggr", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()
    Seq((1, 2, "test test"))
      .toDF("c1", "c2", "part")
      .createOrReplaceTempView("separate_hash_aggr")
    val executePlan = spark.sql("select c1, collect_list(c2) from separate_hash_aggr group by c1")
    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val objectHashAggregateExec =
      plan.executedPlan
        .collectFirst {
          case objectHashAggregateExec: ObjectHashAggregateExec
              if objectHashAggregateExec.aggregateExpressions.forall(_.mode == Partial) =>
            objectHashAggregateExec
        }
    val afterConvertPlan = AuronConverters.convertSparkPlan(objectHashAggregateExec.get)
    assert(afterConvertPlan.isInstanceOf[ObjectHashAggregateExec])
    checkAnswer(executePlan, Seq(Row(1, ArrayBuffer(2))))
  }

}
