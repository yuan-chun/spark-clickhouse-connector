package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.Decimal


case class OlapRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    logInfo("开始应用 OlapRule 优化规则")
    plan transformAllExpressions {
      case Multiply(left, right, _) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.isInstanceOf[Decimal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Decimal].toDouble == 1.0 =>
        logInfo("OlapRule 优化规则生效")
        left
    }
  }
}

class OlapExtension extends (SparkSessionExtensions => Unit) with Logging {
  def apply(e: SparkSessionExtensions): Unit = {
    logInfo("进入 OlapExtension 扩展点")
    e.injectOptimizerRule(OlapV2ScanRelationPushDown)
  }
}

