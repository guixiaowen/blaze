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
package org.apache.spark.sql.auron.iceberg

import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.AuronConverters.getBooleanConf
import org.apache.spark.sql.auron.{AuronConvertProvider, AuronConverters}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class IcebergConvertProvider  extends AuronConvertProvider with Logging{

  override def isEnabled: Boolean = {
    AuronConverters.getBooleanConf("spark.auron.enable.iceberg", defaultValue = false)
  }

  def enableBatchScan: Boolean =
    getBooleanConf("spark.auron.enable.batch.scan", defaultValue = true)

  override def isSupported(exec: SparkPlan): Boolean = {
    exec match {
      case e: BatchScanExec if enableBatchScan =>
//        if e.relation.tableMeta.storage.serde.isDefined
//          && e.relation.tableMeta.storage.serde.get.contains("Paimon") =>
        true
      case _ => false
    }
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    exec
  }
}
