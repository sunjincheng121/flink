package org.apache.flink.table.api

import org.apache.calcite.rel.RelNode
import org.apache.flink.table.calcite.FlinkRelBuilder
import org.apache.flink.table.plan.logical.LogicalNode

trait InnerTable extends Table {
  def logicalPlan: LogicalNode
  def tableEnv: TableEnvironment
  def relBuilder: FlinkRelBuilder
  def getRelNode: RelNode
}

