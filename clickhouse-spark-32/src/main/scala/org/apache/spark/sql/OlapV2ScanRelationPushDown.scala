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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral

import scala.collection.mutable
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, AttributeSet, BinaryComparison, Cast, EmptyRow, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NamedExpression, NonNullLiteral, Not, Or, PredicateHelper, ProjectionOverSchema, SchemaPruning, SubqueryExpression, aggregate}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, quoteIdentifier}
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownAggregates, SupportsPushDownFilters, SupportsPushDownRequiredColumns, V1Scan}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, PushableColumnWithoutNestedColumn}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, PushDownUtils}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DataTypes, DoubleType, FloatType, IntegerType, LongType, Metadata, NumericType, ShortType, StringType, StructType}
import org.apache.spark.sql.util.SchemaUtils._
import org.apache.spark.unsafe.types.UTF8String

import java.util.{HashMap => JHashMap}
import xenon.clickhouse.read.{ClickHouseScanBuilder, SupportPushDimAndMeasure}

case class OlapV2ScanRelationPushDown(spark: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

  def apply(plan: LogicalPlan): LogicalPlan = {
    applyColumnPruning(pushDownAggregates(pushDownFilters(createScanBuilder(plan))))
  }

  private def createScanBuilder(plan: LogicalPlan) = plan.transform {
    case r: DataSourceV2Relation =>
      ScanBuilderHolder(r.output, r, r.table.asReadable.newScanBuilder(r.options))
  }


  def removeCast(expression: Expression): Expression = {
    expression match {
      case Cast(child, _, _, _) => {
        removeCast(child)
      }
      case other => other.mapChildren {
        case Cast(child, _, _, _) => {
          if (child.find {
            case _: Cast => true
            case _ => false
          }.isEmpty)
            child
          else
            removeCast(child)
        }
        case o => removeCast(o)
      }
    }
  }

  def removeQualifier(expression: Expression): Expression = {
    expression match {
      case AttributeReference(name, d, nullable, metadata) => {
        AttributeReference(name, d, nullable, metadata)(NamedExpression.newExprId, Seq.empty[String])
      }
      case other => other.mapChildren {
        case AttributeReference(name, d, nullable, metadata) => {
          AttributeReference(name, d, nullable, metadata)(NamedExpression.newExprId, Seq.empty[String])
        }
        case o => removeQualifier(o)
      }
    }
  }

  def removeAlias(expression: Expression): Expression = {
    expression match {
      case Alias(child, _) => {
        removeAlias(child)
      }
      case other => other.mapChildren {
        case Alias(child, _) => {
          child
        }
        case o => removeAlias(o)
      }
    }
  }


  private def pushDownFilters(plan: LogicalPlan) = plan.transform {
    // update the scan builder with filter push down and return a new plan with filter pushed
    case Filter(condition, sHolder: ScanBuilderHolder) if sHolder.builder.isInstanceOf[SupportsPushDownFilters] && sHolder.builder.isInstanceOf[ClickHouseScanBuilder] =>
      val expression = removeQualifier(removeCast(condition.clone()))
      logInfo(s"filter expression sql ${expression.sql}")
      val filters = splitConjunctivePredicates(expression)

      val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
      // Catalyst filter expression that can't be translated to data source filters.
      val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

      for (filterExpr <- filters) {
        val translated = translateFilter(filterExpr)
        if (translated.isEmpty) {
          untranslatableExprs += filterExpr
        } else {
          translatedFilters += translated.get
        }
      }
      sHolder.builder.asInstanceOf[SupportsPushDownFilters].pushFilters(translatedFilters.toArray)

      val normalizedFilters =
        DataSourceStrategy.normalizeExprs(filters, sHolder.relation.output)
      val (normalizedFiltersWithSubquery, _) =
        normalizedFilters.partition(SubqueryExpression.hasSubquery)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) = (translatedFilters, Nil)

      val postScanFilters = postScanFiltersWithoutSubquery ++ normalizedFiltersWithSubquery

      logInfo(
        s"""
           |OlapPushing operators to ${sHolder.relation.name}
           |OlapPushed Filters: ${pushedFilters.mkString(", ")}
           |OlapPost-Scan Filters: ${postScanFilters.mkString(",")}
         """.stripMargin)
      //remove filter
      sHolder
  }

  def and(exprs: Seq[Expression]): Option[Expression] = exprs.size match {
    case 0 => None
    case 1 => exprs.headOption
    case _ => Some(exprs.tail.fold(exprs.head)(And))
  }

  def translateFilter(filter: Expression): Option[sources.Filter] = {
    filter match {
      case catalyst.expressions.EqualTo(FilterColumnExtractor(column), Literal(v, t)) =>
        Some(sources.EqualTo(column._1, convertToScala(v, t)))
      case catalyst.expressions.EqualTo(Literal(v, t), FilterColumnExtractor(column)) =>
        Some(sources.EqualTo(column._1, convertToScala(v, t)))

      case catalyst.expressions.EqualNullSafe(FilterColumnExtractor(column), Literal(v, t)) =>
        Some(sources.EqualNullSafe(column._1, convertToScala(v, t)))
      case catalyst.expressions.EqualNullSafe(Literal(v, t), FilterColumnExtractor(column)) =>
        Some(sources.EqualNullSafe(column._1, convertToScala(v, t)))

      case catalyst.expressions.GreaterThan(FilterColumnExtractor(column), literal: Literal) =>
        val (v, bool) = changeSqlTime(true, column._2, literal)
        Some(sources.GreaterThan(column._1, v))
      case catalyst.expressions.GreaterThan(literal: Literal, FilterColumnExtractor(column)) =>
        val (v, bool) = changeSqlTime(true, column._2, literal)
        Some(sources.GreaterThan(column._1, v))

      case catalyst.expressions.LessThan(FilterColumnExtractor(column), literal: Literal) =>
        val (v, bool) = changeSqlTime(false, column._2, literal)
        Some(sources.LessThan(column._1, v))
      case catalyst.expressions.LessThan(literal: Literal, FilterColumnExtractor(column)) =>
        val (v, bool) = changeSqlTime(false, column._2, literal)
        Some(sources.LessThan(column._1, v))

      case catalyst.expressions.GreaterThanOrEqual(
      FilterColumnExtractor(column), literal: Literal) =>
        val (v, bool) = changeSqlTime(true, column._2, literal)
        Some(sources.GreaterThanOrEqual(column._1, v))

      case catalyst.expressions.LessThanOrEqual(FilterColumnExtractor(column), literal: Literal) =>
        val (v, bool) = changeSqlTime(false, column._2, literal)
        Some(sources.LessThanOrEqual(column._1, v))

      case catalyst.expressions.LessThanOrEqual(literal: Literal, FilterColumnExtractor(column)) =>
        val (v, bool) = changeSqlTime(false, column._2, literal)
        Some(sources.LessThanOrEqual(column._1, v))

      case catalyst.expressions.InSet(FilterColumnExtractor(column), set) =>
        val toScala = CatalystTypeConverters.createToScalaConverter(column._2.dataType)
        Some(sources.In(column._1, set.toArray.map(toScala)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case catalyst.expressions.In(
      FilterColumnExtractor(column), list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(column._2.dataType)
        Some(sources.In(column._1, hSet.toArray.map(toScala)))

      case catalyst.expressions.IsNull(FilterColumnExtractor(column)) =>
        Some(sources.IsNull(column._1))
      case catalyst.expressions.IsNotNull(FilterColumnExtractor(column)) =>
        Some(sources.IsNotNull(column._1))
      case catalyst.expressions.And(left, right) =>
        (translateFilter(left) ++ translateFilter(right)).reduceOption(sources.And)
      case catalyst.expressions.Or(left, right) =>
        for {
          leftFilter <- translateFilter(left)
          rightFilter <- translateFilter(right)
        } yield sources.Or(leftFilter, rightFilter)

      case catalyst.expressions.Not(child) =>
        translateFilter(child) map (t => sources.Not(t))

      case catalyst.expressions.StartsWith(
      FilterColumnExtractor(column), Literal(v: UTF8String, StringType)) =>
        Some(sources.StringStartsWith(column._1, v.toString))

      case catalyst.expressions.EndsWith(
      FilterColumnExtractor(column), Literal(v: UTF8String, StringType)) =>
        Some(sources.StringEndsWith(column._1, v.toString))

      case catalyst.expressions.Contains(
      FilterColumnExtractor(column), Literal(v: UTF8String, StringType)) =>
        Some(sources.StringContains(column._1, v.toString))

      case catalyst.expressions.ArrayContains(FilterColumnExtractor(column), Literal(value, _)) =>
        Some(sources.StringContains(column._1, value.toString))
      case _ => None
    }
  }

  def changeSqlTime(isGreater: Boolean, a: Expression, literal: Literal): (Any, Boolean) = {
    a match {
      case _ => (convertToScala(literal.value, literal.dataType), false)
    }
  }

  def pushAggregates(
                      scanBuilder: ScanBuilder,
                      aggregates: Seq[AggregateExpression],
                      groupBy: Seq[Expression]): Option[Aggregation] = {

    def columnAsString(e: Expression): Option[FieldReference] = e match {
      case PushableColumnWithoutNestedColumn(name) =>
        val value = new FieldReference(Seq(name))
        Some(value)
      case _ => None
    }

    scanBuilder match {
      case r: SupportsPushDownAggregates if aggregates.nonEmpty =>
        val translatedAggregates = aggregates.flatMap(DataSourceStrategy.translateAggregate)
        val translatedGroupBys = groupBy.flatMap(columnAsString)

        if (translatedAggregates.length != aggregates.length ||
          translatedGroupBys.length != groupBy.length) {
          return None
        }

        val agg = new Aggregation(translatedAggregates.toArray, translatedGroupBys.toArray)
        Some(agg).filter(r.pushAggregation)
      case _ => None
    }
  }

  def pushDownAggregates(plan: LogicalPlan): LogicalPlan = plan.transform {
    // update the scan builder with agg pushdown and return a new plan with agg pushed
    case aggNode@Aggregate(groupingExpressions, resultExpressions, child) =>
      child match {
        case ScanOperation(project, filters, sHolder: ScanBuilderHolder)
          if filters.isEmpty =>
          sHolder.builder match {
            case _: SupportsPushDownAggregates =>
              val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
              var ordinal = 0
              val aggregates = resultExpressions.flatMap { expr =>
                expr.collect {
                  // Do not push down duplicated aggregate expressions. For example,
                  // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
                  // `max(a)` to the data source.
                  case agg: AggregateExpression
                    if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
                    aggExprToOutputOrdinal(agg.canonicalized) = ordinal
                    ordinal += 1
                    agg
                }
              }
              val normalizedAggregates = DataSourceStrategy.normalizeExprs(
                aggregates, sHolder.relation.output).asInstanceOf[Seq[AggregateExpression]]
              val normalizedGroupingExpressions = DataSourceStrategy.normalizeExprs(
                groupingExpressions, sHolder.relation.output)
              //去除别名，尝试下推ck
              val expressions = normalizedGroupingExpressions.map {
                case attr@AttributeReference(name, dataType, nullable, metadata) if "_groupingexpression".equals(name) => {
                  project.find(a => a.exprId == attr.exprId) match {
                    case Some(Alias(a, _)) => AttributeReference(removeCast(removeQualifier(a.clone())).sql, dataType, nullable, metadata)(attr.exprId, attr.qualifier)
                  }
                }
                case other => other
              }

              val pushedAggregates = pushAggregates(
                sHolder.builder, normalizedAggregates, expressions)
              if (pushedAggregates.isEmpty) {
                aggNode // return original plan node
              } else {
                // No need to do column pruning because only the aggregate columns are used as
                // DataSourceV2ScanRelation output columns. All the other columns are not
                // included in the output.
                val scan = sHolder.builder.build()

                // scalastyle:off
                // use the group by columns and aggregate columns as the output columns
                // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                // SELECT min(c1), max(c1) FROM t GROUP BY c2;
                // Use c2, min(c1), max(c1) as output for DataSourceV2ScanRelation
                // We want to have the following logical plan:
                // == Optimized Logical Plan ==
                // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                // +- RelationV2[c2#10, min(c1)#21, max(c1)#22]
                // scalastyle:on
                val newOutput = scan.readSchema().toAttributes
                assert(newOutput.length == groupingExpressions.length + aggregates.length)
                val groupAttrs = normalizedGroupingExpressions.zip(newOutput).map {
                  case (a: Attribute, b: Attribute) => b.withExprId(a.exprId)
                  case (_, b) => b
                }
                val output = groupAttrs ++ newOutput.drop(groupAttrs.length)

                logInfo(
                  s"""
                     |Pushing operators to ${sHolder.relation.name}
                     |Pushed Aggregate Functions:
                     | ${pushedAggregates.get.aggregateExpressions.mkString(", ")}
                     |Pushed Group by:
                     | ${pushedAggregates.get.groupByColumns.mkString(", ")}
                     |Output: ${output.mkString(", ")}
                      """.stripMargin)

                val wrappedScan = getWrappedScan(scan, sHolder, pushedAggregates)

                val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

                val plan = Aggregate(
                  output.take(groupingExpressions.length), resultExpressions, scanRelation)

                // scalastyle:off
                // Change the optimized logical plan to reflect the pushed down aggregate
                // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
                // SELECT min(c1), max(c1) FROM t GROUP BY c2;
                // The original logical plan is
                // Aggregate [c2#10],[min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
                // +- RelationV2[c1#9, c2#10] ...
                //
                // After change the V2ScanRelation output to [c2#10, min(c1)#21, max(c1)#22]
                // we have the following
                // !Aggregate [c2#10], [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
                // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
                //
                // We want to change it to
                // == Optimized Logical Plan ==
                // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
                // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
                // scalastyle:on
                val aggOutput = output.drop(groupAttrs.length)
                plan.transformExpressions {
                  case agg: AggregateExpression =>
                    val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
                    val aggFunction: aggregate.AggregateFunction =
                      agg.aggregateFunction match {
                        case max: aggregate.Max => max.copy(child = aggOutput(ordinal))
                        case min: aggregate.Min => min.copy(child = aggOutput(ordinal))
                        case sum: aggregate.Sum => sum.copy(child = aggOutput(ordinal))
                        case _: aggregate.Count => aggregate.Sum(aggOutput(ordinal))
                        case other => other
                      }
                    agg.copy(aggregateFunction = aggFunction)
                }
              }
            case _ => aggNode
          }
        case _ => aggNode
      }
  }

  def pruneColumns(
                    scanBuilder: ScanBuilder,
                    relation: DataSourceV2Relation,
                    projects: Seq[NamedExpression],
                    dimOrMeasure2Project: JHashMap[String, Array[String]]): (Scan, Seq[AttributeReference]) = {
    //去除别名，下推
    val references = projects.map {
      case a: AttributeReference => a
      case Alias(Cast(child, dataType, _, _), _) =>
        child match {
          case a: AttributeReference =>
            AttributeReference(a.name, dataType, metadata = a.metadata)(a.exprId, Seq())
        }
      case Alias(child, _) =>
        child match {
          case a: AttributeReference =>
            AttributeReference(a.name, a.dataType, metadata = a.metadata)(a.exprId, Seq())
        }
    }

    logInfo(s"dimOrMeasure2Project ${dimOrMeasure2Project}")
    scanBuilder match {
      case r: SupportPushDimAndMeasure if scanBuilder.isInstanceOf[ClickHouseScanBuilder] =>
        r.pruneColumns(references.toStructType)
        r.pushDimAndMeasure(dimOrMeasure2Project)
        val scan = r.build()
        scan -> references
      case r: SupportsPushDownRequiredColumns if scanBuilder.isInstanceOf[ClickHouseScanBuilder] =>
        r.pruneColumns(references.toStructType)
        val scan = r.build()
        scan -> references
      case _ => scanBuilder.build() -> relation.output
    }
  }

  def applyColumnPruning(plan: LogicalPlan): LogicalPlan = {
//    val hasAggFunInGrouping = plan.find {
//      case Aggregate(_, _, child) =>
//        child match {
//          case ScanOperation(project, filters, _) =>
//            if (project.exists(!_.isInstanceOf[AttributeReference]))
//              true
//            else
//              false
//          case _ => false
//        }
//      case _ => false
//    }.isDefined
    plan.transform {
      case ScanOperation(project, filters, sHolder: ScanBuilderHolder) =>
        // column pruning
        val normalizedProjects =
          project.map {
            case a: AttributeReference => a
            case a@Alias(Cast(child, dataType, _, _), name) =>
              val reference = child match {
                case a: AttributeReference =>
                  AttributeReference(a.name, dataType, metadata = a.metadata)(a.exprId, a.qualifier)
                case other =>
                  AttributeReference(removeQualifier(removeCast(other)).sql, dataType, metadata = Metadata.empty)()
              }
              Alias(reference, name)(exprId = a.exprId, qualifier = a.qualifier)
            case a@Alias(child, name) =>
              val reference = child match {
                case a: AttributeReference =>
                  AttributeReference(a.name, a.dataType, metadata = a.metadata)(a.exprId, a.qualifier)
                case other =>
                  val expression = removeQualifier(removeCast(other))
                  val dataType = expression.dataType
                  val sql = expression.sql
                  AttributeReference(sql, dataType, metadata = Metadata.empty)()
              }
              Alias(reference, name)(exprId = a.exprId, qualifier = a.qualifier)
          }

        val exprs = project ++ filters
        val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val dimOrMeasure2Project = new JHashMap[String, Array[String]]
        exprs.foreach { outPut =>
          outPut foreach {
            case attr: AttributeReference if requiredColumns.contains(attr) =>
              dimOrMeasure2Project.merge(removeQualifier(removeCast(attr.clone())).sql, Array() :+ removeQualifier(removeCast(removeAlias(outPut.clone()))).sql, (old, _) => old :+ removeQualifier(removeCast(removeAlias(outPut.clone()))).sql)
            case _ =>
          }
        }
        val (scan, output) = pruneColumns(sHolder.builder, sHolder.relation, normalizedProjects, dimOrMeasure2Project)

        logInfo(
          s"""
             |Output: ${output.mkString(", ")}
         """.stripMargin)

        val wrappedScan = getWrappedScan(scan, sHolder, Option.empty[Aggregation])

        val scanRelation = DataSourceV2ScanRelation(sHolder.relation, wrappedScan, output)

        //初始化下推字段的schema到projectionOverSchema,用于填充dataType
        val projectionOverSchema = ProjectionOverSchema(output.toStructType)
        val projectionFunc = (expr: Expression) => expr transformDown {
          case projectionOverSchema(newExpr) => newExpr
        }

        val filterCondition = filters.reduceLeftOption(And)
        val newFilterCondition = filterCondition.map(projectionFunc)
        val withFilter = newFilterCondition.map(Filter(_, scanRelation)).getOrElse(scanRelation)

        val newProjects = normalizedProjects
          .map(projectionFunc)
          .asInstanceOf[Seq[NamedExpression]]
        Project(restoreOriginalOutputNames(newProjects, project.map(_.name)), withFilter)
    }
  }

  private def getWrappedScan(
                              scan: Scan,
                              sHolder: ScanBuilderHolder,
                              aggregation: Option[Aggregation]): Scan = {
    scan match {
      case v1: V1Scan =>
        val pushedFilters = sHolder.builder match {
          case f: SupportsPushDownFilters =>
            f.pushedFilters()
          case _ => Array.empty[sources.Filter]
        }
        V1ScanWrapper(v1, pushedFilters, aggregation)
      case _ => scan
    }
  }
}

case class ScanBuilderHolder(
                              output: Seq[AttributeReference],
                              relation: DataSourceV2Relation,
                              builder: ScanBuilder) extends LeafNode

// A wrapper for v1 scan to carry the translated filters and the handled ones. This is required by
// the physical v1 scan node.
case class V1ScanWrapper(
                          v1Scan: V1Scan,
                          handledFilters: Seq[sources.Filter],
                          pushedAggregate: Option[Aggregation]) extends Scan {
  override def readSchema(): StructType = v1Scan.readSchema()
}


object FilterColumnExtractor {
  def unapply(e: Expression): Option[(String, Expression)] = e match {
    case a: Attribute => Some((a.name, e))
    case Cast(n: AttributeReference, _, _, _) => {
      n.withQualifier(Seq.empty[String])
      Some((n.name, n))
    }
    case otherExpr => Some(otherExpr.sql, otherExpr)
  }
}