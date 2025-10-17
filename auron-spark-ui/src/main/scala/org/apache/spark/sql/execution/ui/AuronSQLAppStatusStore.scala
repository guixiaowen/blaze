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
package org.apache.spark.sql.execution.ui

import scala.jdk.CollectionConverters.asScalaIteratorConverter

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVIndex, KVStore, KVStoreView}

class AuronSQLAppStatusStore(store: KVStore) {

  private def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator())(iter => iter.asScala.toList)
  }

  def executionsList(): Seq[AuronSQLExecutionUIData] = {
    viewToSeq(store.view(classOf[AuronSQLExecutionUIData]))
  }

  def buildInfo(): AuronBuildInfoUIData = {
    val kClass = classOf[AuronBuildInfoUIData]
    store.read(kClass, kClass.getName)
  }

  def executionsList(offset: Int, length: Int): Seq[AuronSQLExecutionUIData] = {
    viewToSeq(store.view(classOf[AuronSQLExecutionUIData]).skip(offset).max(length))
  }

  def execution(executionId: Long): Option[AuronSQLExecutionUIData] = {
    try {
      Some(store.read(classOf[AuronSQLExecutionUIData], executionId))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  def executionsCount(): Long = {
    store.count(classOf[AuronSQLExecutionUIData])
  }
}

@KVIndex("executionId")
class AuronSQLExecutionUIData(
    @KVIndexParam val executionId: Long,
    val description: String,
    val numAuronNodes: Int,
    val numFallbackNodes: Int,
    val fallbackDescription: String,
    val fallbackNodeToReason: Seq[(String, String)]) {}

class AuronBuildInfoUIData(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[AuronBuildInfoUIData].getName()
}
