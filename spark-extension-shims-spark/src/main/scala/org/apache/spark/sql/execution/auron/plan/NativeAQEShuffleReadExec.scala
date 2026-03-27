package org.apache.spark.sql.execution.auron.plan

import org.apache.auron.sparkver
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}

case class NativeAQEShuffleReadExec(override val child: SparkPlan, partitionSpecs: Seq[ShufflePartitionSpec])
  extends NativeAQEShuffleReadBase(child: SparkPlan,
  partitionSpecs: Seq[ShufflePartitionSpec]) {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5 / 4.0 / 4.1")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
