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

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanExec

class AuronSparkSessionExtensionSuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {

  test("test Optimize preColumnarTransitions with FileSourceScanExec") {
    withTable("file_source_scan") {
      sql(
        "create table file_source_scan using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val executedPlan = sql("select * from file_source_scan").queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
        .executedPlan

      val afterPlan = AuronColumnarOverrides
        .apply(spark)
        .preColumnarTransitions
        .apply(executedPlan)
      assert(
        afterPlan
          .isInstanceOf[NativeParquetScanExec])
    }
  }

}
