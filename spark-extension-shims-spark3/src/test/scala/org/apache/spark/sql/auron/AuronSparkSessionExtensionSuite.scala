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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{SparkSession, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.{FileSourceScanExec, LocalTableScanExec}
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanExec
import org.apache.spark.sql.execution.benchmark.BuiltInDataSourceWriteBenchmark.withTable

class AuronSparkSessionExtensionSuite extends SparkFunSuite with SQLHelper {

  private def create(
      builder: SparkSessionExtensionsProvider): Seq[SparkSessionExtensionsProvider] = Seq(builder)

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(builders: Seq[SparkSessionExtensionsProvider])(
      f: SparkSession => Unit): Unit = {
    val builder =
      SparkSession
        .builder()
        .master("local[1]")
        .config("spark.sql.catalogImplementation", "hive")
    builders.foreach(builder.withExtensions)
    val spark = builder.getOrCreate()
    try f(spark)
    finally {
      stop(spark)
    }
  }
  test("test Optimize preColumnarTransitions with FileSourceScanExec") {
    val extensions = create { extensions =>
      extensions.injectColumnar(sparkSession => {
        AuronColumnarOverrides(sparkSession)
      })
    }

    withSession(extensions) { session =>
      withTable(
        "file_source_scan" +
          "" +
          "") {
        session.sql(
          "create table file_source_scan using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
        val executedPlan =
          session.sql("select * from file_source_scan").queryExecution.executedPlan

        assert(executedPlan.isInstanceOf[NativeParquetScanExec])
      }
    }
  }

  test("test Optimize preColumnarTransitions with LeafExecNode") {
    val extensions = create { extensions =>
      extensions.injectColumnar(sparkSession => {
        AuronColumnarOverrides(sparkSession)
      })
    }

    withSession(extensions) { session =>
      import session.implicits._
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("local_table_scan")
      val df = session.table("local_table_scan")
      val localScan = df.queryExecution.executedPlan.collect { case s: LocalTableScanExec =>
        s
      }

      assert(!localScan(0).getTagValue(AuronConvertStrategy.convertibleTag).isDefined)
    }

  }
}
