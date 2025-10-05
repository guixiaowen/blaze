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

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.internal.StaticSQLConf.UI_RETAINED_EXECUTIONS
import org.apache.spark.status.ElementTrackingStore

import org.apache.auron.spark.ui.AuronBuildInfoEvent

class AuronSQLAppStatusListener(conf: SparkConf, kvstore: ElementTrackingStore)
    extends SparkListener
    with Logging {
  private val executionIdToDescription = new mutable.HashMap[Long, String]

  kvstore.addTrigger(classOf[SQLExecutionUIData], conf.get[Int](UI_RETAINED_EXECUTIONS)) {
    count => cleanupExecutions(count)
  }

  def getAuronBuildInfo(): Long = {
    kvstore.count(classOf[AuronBuildInfoUIData])
  }

  private def onAuronBuildInfo(event: AuronBuildInfoEvent): Unit = {
    val uiData = new AuronBuildInfoUIData(event.info.toSeq)
    kvstore.write(uiData)
  }

  private def onSQLExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    executionIdToDescription.put(event.executionId, event.description)
  }

  private def onSQLExtensionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    executionIdToDescription.remove(event.executionId)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionStart => onSQLExecutionStart(e)
    case e: SparkListenerSQLExecutionEnd => onSQLExtensionEnd(e)
    case e: AuronBuildInfoEvent => onAuronBuildInfo(e)
    case _ => // Ignore
  }

  private def cleanupExecutions(count: Long): Unit = {
    val countToDelete = count - conf.get(UI_RETAINED_EXECUTIONS)
    if (countToDelete <= 0) {
      return
    }
  }

}
object AuronSQLAppStatusListener {
  def register(sc: SparkContext): Unit = {
    val kvStore = sc.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new AuronSQLAppStatusListener(sc.conf, kvStore)
    sc.listenerBus.addToStatusQueue(listener)
  }
}
