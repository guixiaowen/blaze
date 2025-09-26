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
package org.apache.spark.sql.auron.configuration

import org.apache.auron.configuration.{AuronConfiguration, ConfigOption, ConfigOptions}
import org.apache.spark.sql.internal.SQLConf

import java.util
import java.util.Optional

class SparkAuronConfiguration extends AuronConfiguration {

  val sparkAuronConfiguration = new util.HashMap[String, AnyRef]

  override def getOptional[T](option: ConfigOption[T]): Optional[T] = {
    Optional.ofNullable(sparkAuronConfiguration.get(option.key()).asInstanceOf[T])
  }

  override def getOptional[T](key: String): Optional[T] = {
    Optional.ofNullable(sparkAuronConfiguration.get(key).asInstanceOf[T])
  }

  def setSparkAuronConfiguration(): Unit = {
    SQLConf.get.
  }

  val BOOLEAN_CONFIG_OPTION =
    ConfigOptions.key("spark.auron.udf.UDFJson.enabled").booleanType().defaultValue(true)

  val UDF_BRICK_HOUSE_ENABLED =
    ConfigOptions.key("spark.auron.udf.brickhouse.enabled").booleanType().defaultValue(true)

  val DECIMAL_ARITH_OP_ENABLED =
    ConfigOptions.key("spark.auron.decimal.arithOp.enabled").booleanType().defaultValue(true)

  val ENABLED_SCAN =
    ConfigOptions.key("spark.auron.enable.scan").booleanType().defaultValue(true)

  val ENABLED_PROJECT =
    ConfigOptions.key("spark.auron.enable.project").booleanType().defaultValue(true)

  val ENABLED_FILTER =
    ConfigOptions.key("spark.auron.enable.filter").booleanType().defaultValue(true)

  val ENABLED_SORT =
    ConfigOptions.key("spark.auron.enable.sort").booleanType().defaultValue(true)

  val ENABLED_UNION =
    ConfigOptions.key("spark.auron.enable.union").booleanType().defaultValue(true)

  val ENABLED_SMJ =
    ConfigOptions.key("spark.auron.enable.smj").booleanType().defaultValue(true)

  val ENABLED_SHJ =
    ConfigOptions.key("spark.auron.enable.shj").booleanType().defaultValue(true)

  val ENABLED_BHJ =
    ConfigOptions.key("spark.auron.enable.bhj").booleanType().defaultValue(true)

  val ENABLED_BNLJ =
    ConfigOptions.key("spark.auron.enable.bnlj").booleanType().defaultValue(true)

  val ENABLED_LOCAL_LIMIT =
    ConfigOptions.key("spark.auron.enable.local.limit").booleanType().defaultValue(true)

  val ENABLED_GLOBAL_LIMIT =
    ConfigOptions.key("spark.auron.enable.global.limit").booleanType().defaultValue(true)

  val ENABLED_TAKE_ORDERED_AND_PROJECT =
    ConfigOptions.key("spark.auron.enable.take.ordered.and.project").booleanType().defaultValue(true)

  val ENABLED_AGGR =
    ConfigOptions.key("spark.auron.enable.aggr").booleanType().defaultValue(true)

  val ENABLED_EXPAND =
    ConfigOptions.key("spark.auron.enable.expand").booleanType().defaultValue(true)

  val ENABLED_WINDOW =
    ConfigOptions.key("spark.auron.enable.window").booleanType().defaultValue(true)

  val ENABLED_WINDOW_GROUP_LIMIT =
    ConfigOptions.key("spark.auron.enable.window.group.limit").booleanType().defaultValue(true)

  val ENABLED_GENERATE =
    ConfigOptions.key("spark.auron.enable.generate").booleanType().defaultValue(true)

  val ENABLED_LOCAL_TABLE_SCAN =
    ConfigOptions.key("spark.auron.enable.local.table.scan").booleanType().defaultValue(true)

  val ENABLED_DATA_WRITING =
    ConfigOptions.key("spark.auron.enable.data.writing").booleanType().defaultValue(false)

  val ENABLED_SCAN_PARQUET =
    ConfigOptions.key("spark.auron.enable.scan.parquet").booleanType().defaultValue(true)

  val ENABLED_SCAN_ORC =
    ConfigOptions.key("spark.auron.enable.scan.orc").booleanType().defaultValue(true)



}
