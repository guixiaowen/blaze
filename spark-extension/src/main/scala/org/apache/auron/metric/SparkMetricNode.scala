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
package org.apache.auron.metric

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric

case class SparkMetricNode(
    metrics: Map[String, SQLMetric],
    children: Seq[MetricNode],
    metricValueHandler: Option[(String, Long) => Unit] = None)
    extends MetricNode(children.asJava)
    with Logging {

  override def getChild(i: Int): MetricNode = {
    if (i < children.length) {
      children(i)
    } else {
      null
    }
  }

  def add(metricName: String, v: Long): Unit = {
    metricValueHandler.foreach(_.apply(metricName, v))
    if (v > 0) {
      metrics.get(metricName).foreach(_.add(v))
    }
  }

  def foreach(fn: SparkMetricNode => Unit): Unit = {
    fn(this)
    this.children.foreach(_.asInstanceOf[SparkMetricNode].foreach(fn))
  }
}
