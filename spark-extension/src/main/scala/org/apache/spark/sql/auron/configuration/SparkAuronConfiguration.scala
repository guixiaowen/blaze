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

import java.util.Optional

import org.apache.spark.sql.auron.configuration.SparkAuronConfiguration._
import org.apache.spark.sql.internal.SQLConf

import org.apache.auron.configuration.{AuronConfiguration, ConfigOption, ConfigOptions}

class SparkAuronConfiguration(conf: SQLConf) extends AuronConfiguration {

  override def getOptional[T](option: ConfigOption[T]): Optional[T] = {
    Optional.ofNullable(conf.settings.get(option.key()).asInstanceOf[T])
  }

  override def getOptional[T](key: String): Optional[T] = {
    Optional.ofNullable(conf.settings.get(key).asInstanceOf[T])
  }

  def enableScan: Boolean =
    get(ENABLED_SCAN)

  def enableProject: Boolean =
    get(ENABLED_PROJECT)

  def enableFilter: Boolean =
    get(ENABLED_FILTER)

  def enableSort: Boolean =
    get(ENABLED_SORT)

  def enableUnion: Boolean =
    get(ENABLED_UNION)

  def enableSmj: Boolean =
    get(ENABLED_SMJ)

  def enableShj: Boolean =
    get(ENABLED_SHJ)

  def enableBhj: Boolean =
    get(ENABLED_BHJ)

  def enableBnlj: Boolean =
    get(ENABLED_BNLJ)

  def enableLocalLimit: Boolean =
    get(ENABLED_LOCAL_LIMIT)

  def enableGlobalLimit: Boolean =
    get(ENABLED_GLOBAL_LIMIT)

  def enableTakeOrderedAndProject: Boolean =
    get(ENABLED_TAKE_ORDERED_AND_PROJECT)

  def enableAggr: Boolean =
    get(ENABLED_AGGR)

  def enableExpand: Boolean =
    get(ENABLED_EXPAND)

  def enableWindow: Boolean =
    get(ENABLED_WINDOW)

  def enableWindowGroupLimit: Boolean =
    get(ENABLED_WINDOW_GROUP_LIMIT)

  def enableGenerate: Boolean =
    get(ENABLED_GENERATE)

  def enableLocalTableScan: Boolean =
    get(ENABLED_LOCAL_TABLE_SCAN)

  def enableDataWriting: Boolean =
    get(ENABLED_DATA_WRITING)

  def enableScanParquet: Boolean =
    get(ENABLED_SCAN_PARQUET)

  def enableScanOrc: Boolean =
    get(ENABLED_SCAN_ORC)

  def enableUdfJson: Boolean =
    get(UDF_JSON_ENABLED)

  def enableUdfBrickHouse: Boolean =
    get(UDF_BRICK_HOUSE_ENABLED)

  def enableDecimalArithOp: Boolean =
    get(DECIMAL_ARITH_OP_ENABLED)
}

object SparkAuronConfiguration {

  def get: SparkAuronConfiguration = {
    new SparkAuronConfiguration(SQLConf.get)
  }

  val UDF_JSON_ENABLED =
    ConfigOptions
      .key("spark.auron.udf.UDFJson.enabled")
      .description(
        "Enable native implemented get_json_object/json_tuple. May introduce inconsistency in special case (especially with illegal json inputs).")
      .booleanType()
      .defaultValue(true)

  val UDF_BRICK_HOUSE_ENABLED =
    ConfigOptions
      .key("spark.auron.udf.brickhouse.enabled")
      .description("Enable some native-implemented brickhouse UDFs.")
      .booleanType()
      .defaultValue(true)

  val DECIMAL_ARITH_OP_ENABLED =
    ConfigOptions
      .key("spark.auron.decimal.arithOp.enabled")
      .description("spark.auron.decimal.arithOp.enabled")
      .booleanType()
      .defaultValue(true)

  val ENABLED_SCAN =
    ConfigOptions
      .key("spark.auron.enable.scan")
      .description("Enable/disable auron engine for scan.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_PROJECT =
    ConfigOptions
      .key("spark.auron.enable.project")
      .description("Enable/disable converting ProjectExec to NativeProject.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_FILTER =
    ConfigOptions
      .key("spark.auron.enable.filter")
      .description("Enable/disable converting FilterExec to NativeFilter.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_SORT =
    ConfigOptions
      .key("spark.auron.enable.sort")
      .description("Enable/disable converting SortExec to NativeSort.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_UNION =
    ConfigOptions
      .key("spark.auron.enable.union")
      .description("Enable/disable converting UnionExec to NativeUnion.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_SMJ =
    ConfigOptions
      .key("spark.auron.enable.smj")
      .description("Enable/disable converting SortMergeJoinExec to NativeSortMergeJoin.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_SHJ =
    ConfigOptions
      .key("spark.auron.enable.shj")
      .description("Enable/disable converting ShuffledHashJoinExec to NativeShuffledHashJoin.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_BHJ =
    ConfigOptions
      .key("spark.auron.enable.bhj")
      .description("Enable/disable converting BroadcastHashJoinExec to NativeBroadcastHashJoin.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_BNLJ =
    ConfigOptions
      .key("spark.auron.enable.bnlj")
      .description(
        "Enable/disable converting BroadcastNestedLoopJoinExec to NativeBroadcastNestedLoopJoin.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_LOCAL_LIMIT =
    ConfigOptions
      .key("spark.auron.enable.local.limit")
      .description("Enable/disable converting LocalLimitExec to NativeLocalLimit.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_GLOBAL_LIMIT =
    ConfigOptions
      .key("spark.auron.enable.global.limit")
      .description("Enable/disable converting GlobalLimitExec to NativeGlobalLimit.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_TAKE_ORDERED_AND_PROJECT =
    ConfigOptions
      .key("spark.auron.enable.take.ordered.and.project")
      .description(
        "Enable/disable converting TakeOrderedAndProjectExec to NativeTakeOrderedAndProject.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_AGGR =
    ConfigOptions
      .key("spark.auron.enable.aggr")
      .description(
        "Enable/disable converting HashAggregateExec/ObjectHashAggregateExec/SortAggregateExec to NativeAggregate.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_EXPAND =
    ConfigOptions
      .key("spark.auron.enable.expand")
      .description("Enable/disable converting ExpandExec to NativeExpand.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_WINDOW =
    ConfigOptions
      .key("spark.auron.enable.window")
      .description("Enable/disable converting WindowExec to NativeWindow.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_WINDOW_GROUP_LIMIT =
    ConfigOptions
      .key("spark.auron.enable.window.group.limit")
      .description(
        "Enable/disable converting WindowGroupLimitExec to NativeWindowGroupLimitExec.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_GENERATE =
    ConfigOptions
      .key("spark.auron.enable.generate")
      .description("Enable/disable converting GenerateExec to NativeGenerate.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_LOCAL_TABLE_SCAN =
    ConfigOptions
      .key("spark.auron.enable.local.table.scan")
      .description("Enable/disable converting LocalTableScanExec to NativeLocalTableScan.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_DATA_WRITING =
    ConfigOptions
      .key("spark.auron.enable.data.writing")
      .description(
        "Enable/disable converting DataWritingCommandExec to NativeDataWritingCommand.")
      .booleanType()
      .defaultValue(false)

  val ENABLED_SCAN_PARQUET =
    ConfigOptions
      .key("spark.auron.enable.scan.parquet")
      .description("Enable/disable converting FileSourceScanExec of scan parquet to Native.")
      .booleanType()
      .defaultValue(true)

  val ENABLED_SCAN_ORC =
    ConfigOptions
      .key("spark.auron.enable.scan.orc")
      .description("Enable/disable converting FileSourceScanExec of scan orc to Native.")
      .booleanType()
      .defaultValue(true)

}
