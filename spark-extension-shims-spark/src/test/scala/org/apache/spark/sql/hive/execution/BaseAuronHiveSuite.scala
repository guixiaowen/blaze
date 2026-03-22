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
package org.apache.spark.sql.hive.execution


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.test.TestHiveContext

trait BaseAuronHiveSuite {
  protected val suiteWorkspace: String = getClass.getResource("/").getPath + "auron-tests-workdir"
  protected val warehouseDir: String = suiteWorkspace + "/spark-warehouse"
  protected val metastoreDir: String = suiteWorkspace + "/meta"

  protected val spark: SparkSession = TestAuronHive.sparkSession

//  protected def resetSuiteWorkspace(): Unit = {
//    val workdir = new File(suiteWorkspace)
//    if (workdir.exists()) {
//      FileUtils.forceDelete(workdir)
//    }
//    FileUtils.forceMkdir(workdir)
//    FileUtils.forceMkdir(new File(warehouseDir))
//    FileUtils.forceMkdir(new File(metastoreDir))
//  }
//
//  override def beforeAll(): Unit = {
//    // Prepare a clean workspace before SparkSession initialization
//    resetSuiteWorkspace()
//    super.beforeAll()
//    spark.sparkContext.setLogLevel("WARN")
//  }
//
//  override def afterAll(): Unit = {
//    super.afterAll()
//  }

  object TestAuronHive
      extends TestHiveContext(
        new SparkContext(
          System.getProperty("spark.sql.test.master", "local[1]"),
          "TestSQLContext",
          new SparkConf()
            .set("spark.sql.test", "")
            .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
            .set(
              "spark.shuffle.manager",
              "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
            .set("spark.memory.offHeap.enabled", "false")
            .set("spark.auron.enable", "true")
            .set("spark.ui.enabled", "false")
            .set("spark.sql.warehouse.dir", warehouseDir)
            .set("spark.auron.udf.singleChildFallback.enabled", "false")
            .set("spark.auron.enable.parquetHiveTableScanExec", "true")
            .set("spark.sql.hive.convertMetastoreParquet", "false"))) {}
}
