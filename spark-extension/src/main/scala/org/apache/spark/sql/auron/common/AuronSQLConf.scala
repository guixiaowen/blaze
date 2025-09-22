package org.apache.spark.sql.auron.common

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf

object AuronSQLConf extends SQLConf {

  val UDF_UDF_JSON_ENABLED =
    buildConf("spark.auron.udf.UDFJson.enabled")
      .doc("Enable native implemented get_json_object/json_tuple. May introduce inconsistency in special case (especially with illegal json inputs).")
      .booleanConf
      .createWithDefault(true)

  val UDF_BRICK_HOUSE_ENABLED =
    buildConf("spark.auron.udf.brickhouse.enabled")
      .doc("Enable some native-implemented brickhouse UDFs.")
      .booleanConf
      .createWithDefault(true)

  val DECIMAL_ARITH_OP_ENABLED =
    buildConf("spark.auron.decimal.arithOp.enabled")
      .doc("")
      .booleanConf
      .createWithDefault(false)

  val ENABLED_SCAN =
    buildConf("spark.auron.enable.scan")
      .doc("Enable/disable auron engine.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_PROJECT =
    buildConf("spark.auron.enable.project")
      .doc("Enable/disable converting ProjectExec to NativeProject.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_FILTER =
    buildConf("spark.auron.enable.filter")
      .doc("Enable/disable converting FilterExec to NativeFilter.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_SORT =
    buildConf("spark.auron.enable.sort")
      .doc("Enable/disable converting SortExec to NativeSort.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_UNION =
    buildConf("spark.auron.enable.union")
      .doc("Enable/disable converting UnionExec to NativeUnion.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_SMJ =
    buildConf("spark.auron.enable.smj")
      .doc("Enable/disable converting SortMergeJoinExec to NativeSortMergeJoin.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_SHJ =
    buildConf("spark.auron.enable.shj")
      .doc("Enable/disable converting ShuffledHashJoinExec to NativeShuffledHashJoin.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_BHJ =
    buildConf("spark.auron.enable.bhj")
      .doc("Enable/disable converting BroadcastHashJoinExec to NativeBroadcastHashJoin.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_BNLJ =
    buildConf("spark.auron.enable.bnlj")
      .doc("Enable/disable converting BroadcastNestedLoopJoinExec to NativeBroadcastNestedLoopJoin.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_LOCAL_LIMIT =
    buildConf("spark.auron.enable.local.limit")
      .doc("Enable/disable converting LocalLimitExec to NativeLocalLimit.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_GLOBAL_LIMIT =
    buildConf("spark.auron.enable.global.limit")
      .doc("Enable/disable converting GlobalLimitExec to NativeGlobalLimit.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_TAKE_ORDERED_AND_PROJECT =
    buildConf("spark.auron.enable.take.ordered.and.project")
      .doc("Enable/disable converting TakeOrderedAndProjectExec to NativeTakeOrderedAndProject.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_AGGR =
    buildConf("spark.auron.enable.aggr")
      .doc("Enable/disable converting HashAggregateExec/ObjectHashAggregateExec/SortAggregateExec to NativeAggregate.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_EXPAND =
    buildConf("spark.auron.enable.expand")
      .doc("Enable/disable converting ExpandExec to NativeExpand.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_WINDOW =
    buildConf("spark.auron.enable.window")
      .doc("Enable/disable converting WindowExec to NativeWindow.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_WINDOW_GROUP_LIMIT =
    buildConf("spark.auron.enable.window.group.limit")
      .doc("Enable/disable converting WindowGroupLimitExec to NativeWindowGroupLimitExec.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_GENERATE =
    buildConf("spark.auron.enable.generate")
      .doc("Enable/disable converting GenerateExec to NativeGenerate.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_LOCAL_TABLE_SCAN =
    buildConf("spark.auron.enable.local.table.scan")
      .doc("Enable/disable converting LocalTableScanExec to NativeLocalTableScan.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_DATA_WRITING =
    buildConf("spark.auron.enable.data.writing")
      .doc("Enable/disable converting DataWritingCommandExec to NativeDataWritingCommand.")
      .booleanConf
      .createWithDefault(false)

  val ENABLED_SCAN_PARQUET =
    buildConf("spark.auron.enable.scan.parquet")
      .doc("Enable/disable converting FileSourceScanExec of scan parquet to Native.")
      .booleanConf
      .createWithDefault(true)

  val ENABLED_SCAN_ORC =
    buildConf("spark.auron.enable.scan.orc")
      .doc("Enable/disable converting FileSourceScanExec of scan orc to Native.")
      .booleanConf
      .createWithDefault(true)

}

