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
package org.apache.spark.sql.auron;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv$;

@SuppressWarnings("unused")
public enum AuronConf {
    // support spark.auron.ui.enabled
    UI_ENABLED("spark.auron.ui.enabled", true),

    /// suggested batch size for arrow batches.
    BATCH_SIZE("spark.auron.batchSize", 10000),

    /// suggested fraction of off-heap memory used in native execution.
    /// actual off-heap memory usage is expected to be spark.executor.memoryOverhead * fraction.
    MEMORY_FRACTION("spark.auron.memoryFraction", 0.6),

    /// suggested fraction of process total memory (on-heap and off-heap).
    /// this limit is for process's resident memory usage
    PROCESS_MEMORY_FRACTION("spark.auron.process.vmrss.memoryFraction", 0.9),

    /// enable converting upper/lower functions to native, special cases may provide different
    /// outputs from spark due to different unicode versions.
    CASE_CONVERT_FUNCTIONS_ENABLE("spark.auron.enable.caseconvert.functions", true),

    /// enable extra metrics of input batch statistics
    INPUT_BATCH_STATISTICS_ENABLE("spark.auron.enableInputBatchStatistics", true),

    /// supports UDAF and other aggregate functions not implemented
    UDAF_FALLBACK_ENABLE("spark.auron.udafFallback.enable", true),

    // TypedImperativeAggregate one row mem use size
    SUGGESTED_UDAF_ROW_MEM_USAGE("spark.auron.suggested.udaf.memUsedSize", 64),

    /// number of udafs to trigger sort-based aggregation
    /// by default, all aggs containing udafs are converted to sort-based
    UDAF_FALLBACK_NUM_UDAFS_TRIGGER_SORT_AGG("spark.auron.udafFallback.num.udafs.trigger.sortAgg", 1),

    // TypedImperativeAggregate one row mem use size
    UDAF_FALLBACK_ESTIM_ROW_SIZE("spark.auron.udafFallback.typedImperativeEstimatedRowSize", 256),

    /// enable trimming string inputs before casting to numeric/boolean types
    CAST_STRING_TRIM_ENABLE("spark.auron.cast.trimString", true),

    /// ignore corrupted input files
    IGNORE_CORRUPTED_FILES("spark.files.ignoreCorruptFiles", false),

    /// enable partial aggregate skipping (see https://github.com/apache/auron/issues/327)
    PARTIAL_AGG_SKIPPING_ENABLE("spark.auron.partialAggSkipping.enable", true),

    /// partial aggregate skipping ratio
    PARTIAL_AGG_SKIPPING_RATIO("spark.auron.partialAggSkipping.ratio", 0.9),

    /// minimum number of rows to trigger partial aggregate skipping
    PARTIAL_AGG_SKIPPING_MIN_ROWS("spark.auron.partialAggSkipping.minRows", BATCH_SIZE.intConf() * 5),

    /// always skip partial aggregate when triggered spilling
    PARTIAL_AGG_SKIPPING_SKIP_SPILL("spark.auron.partialAggSkipping.skipSpill", false),

    // parquet enable page filtering
    PARQUET_ENABLE_PAGE_FILTERING("spark.auron.parquet.enable.pageFiltering", false),

    // parquet enable bloom filter
    PARQUET_ENABLE_BLOOM_FILTER("spark.auron.parquet.enable.bloomFilter", false),

    // parquet max over read size
    PARQUET_MAX_OVER_READ_SIZE("spark.auron.parquet.maxOverReadSize", 16384),

    // parquet metadata cache size
    PARQUET_METADATA_CACHE_SIZE("spark.auron.parquet.metadataCacheSize", 5),

    // spark io compression codec
    SPARK_IO_COMPRESSION_CODEC("spark.io.compression.codec", "lz4"),

    // spark io compression zstd level
    SPARK_IO_COMPRESSION_ZSTD_LEVEL("spark.io.compression.zstd.level", 1),

    // tokio worker threads per cpu (spark.task.cpus), 0 for auto detection
    TOKIO_WORKER_THREADS_PER_CPU("spark.auron.tokio.worker.threads.per.cpu", 0),

    // number of cpus per task
    SPARK_TASK_CPUS("spark.task.cpus", 1),

    // replace all sort-merge join to shuffled-hash join, only used for benchmarking
    FORCE_SHUFFLED_HASH_JOIN("spark.auron.forceShuffledHashJoin", false),

    // shuffle compression target buffer size, default is 4MB
    SHUFFLE_COMPRESSION_TARGET_BUF_SIZE("spark.auron.shuffle.compression.targetBufSize", 4194304),

    // spark spill compression codec
    SPILL_COMPRESSION_CODEC("spark.auron.spill.compression.codec", "lz4"),

    // enable hash join falling back to sort merge join when hash table is too big
    SMJ_FALLBACK_ENABLE("spark.auron.smjfallback.enable", false),

    // smj fallback threshold
    SMJ_FALLBACK_ROWS_THRESHOLD("spark.auron.smjfallback.rows.threshold", 10000000),

    // smj fallback threshold
    SMJ_FALLBACK_MEM_SIZE_THRESHOLD("spark.auron.smjfallback.mem.threshold", 134217728),

    // max memory fraction of on-heap spills
    ON_HEAP_SPILL_MEM_FRACTION("spark.auron.onHeapSpill.memoryFraction", 0.9),

    // suggested memory size for record batch
    SUGGESTED_BATCH_MEM_SIZE("spark.auron.suggested.batch.memSize", 8388608),

    // fallback to UDFJson when error parsing json in native implementation
    PARSE_JSON_ERROR_FALLBACK("spark.auron.parseJsonError.fallback", true),

    // suggested memory size for k-way merging
    // use smaller batch memory size for kway merging since there will be multiple
    // batches in memory at the same time
    SUGGESTED_BATCH_MEM_SIZE_KWAY_MERGE("spark.auron.suggested.batch.memSize.multiwayMerging", 1048576),

    ORC_FORCE_POSITIONAL_EVOLUTION("spark.auron.orc.force.positional.evolution", false),

    NATIVE_LOG_LEVEL("spark.auron.native.log.level", "info");

    public final String key;
    private final Object defaultValue;

    AuronConf(String key, Object defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public boolean booleanConf() {
        return conf().getBoolean(key, (boolean) defaultValue);
    }

    public int intConf() {
        return conf().getInt(key, (int) defaultValue);
    }

    public long longConf() {
        return conf().getLong(key, (long) defaultValue);
    }

    public double doubleConf() {
        return conf().getDouble(key, (double) defaultValue);
    }

    public String stringConf() {
        return conf().get(key, (String) defaultValue);
    }

    public static boolean booleanConf(String confName) {
        return AuronConf.valueOf(confName).booleanConf();
    }

    public static int intConf(String confName) {
        return AuronConf.valueOf(confName).intConf();
    }

    public static long longConf(String confName) {
        return AuronConf.valueOf(confName).longConf();
    }

    public static double doubleConf(String confName) {
        return AuronConf.valueOf(confName).doubleConf();
    }

    public static String stringConf(String confName) {
        return AuronConf.valueOf(confName).stringConf();
    }

    private static SparkConf conf() {
        return SparkEnv$.MODULE$.get().conf();
    }
}
