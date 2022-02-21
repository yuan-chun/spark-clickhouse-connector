/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.read

import java.time.ZoneId
import scala.util.Using
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.clickhouse.ReadOptions
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import xenon.clickhouse.{ClickHouseHelper, Logging, SQLHelper}
import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.grpc.{GrpcNodeClient, GrpcNodesClient}
import xenon.clickhouse.spec._

import java.util
import java.util.{HashMap => JHashMap}
import scala.util.control.NonFatal

class ClickHouseScanBuilder(
                             scanJob: ScanJobDescription,
                             physicalSchema: StructType,
                             metadataSchema: StructType,
                             partitionTransforms: Array[Transform]
                           ) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownAggregates
  with SupportPushDimAndMeasure
  with ClickHouseHelper
  with SQLHelper

  with Logging {

  implicit private val tz: ZoneId = scanJob.tz


  val database: String = scanJob.tableEngineSpec match {
    case dist: DistributedEngineSpec if scanJob.readOptions.convertDistributedToLocal => dist.local_db
    case _ => scanJob.tableSpec.database
  }

  val table: String = scanJob.tableEngineSpec match {
    case dist: DistributedEngineSpec if scanJob.readOptions.convertDistributedToLocal => dist.local_table
    case _ => scanJob.tableSpec.name
  }

  private val reservedMetadataSchema: StructType = StructType(
    metadataSchema.dropWhile(field => physicalSchema.fields.map(_.name).contains(field.name))
  )

  private var _readSchema: StructType = StructType(
    physicalSchema.fields ++ reservedMetadataSchema.fields
  )
  private var pushedAggregateList: Array[String] = Array()
  private var pushedGroupByCols: Option[Array[String]] = None
  private var dimOrMeasure2Project = new JHashMap[String, Array[String]]

  private var _pushedFilters = Array.empty[Filter]
  private var groupByClause: String = ""

  override def pushedFilters: Array[Filter] = this._pushedFilters

  // filters 是spark处理过的 translatedFilters 可以翻译的filter，不能处理带函数的filter org.apache.spark.sql.execution.datasources.DataSourceStrategy.translateFilterWithMapping
  // 没有没办法把所有条件都下推，可以加优化器解决,改变传入 filters
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (pushed, unSupported) = filters.partition(f => compileFilter(f).isDefined)
    this._pushedFilters = pushed
    unSupported
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    //    val requiredCols = requiredSchema.map(_.name)
    //    this._readSchema = StructType(_readSchema.filter(field => requiredCols.contains(field.name)))
    this._readSchema = requiredSchema
  }

  override def pushDimAndMeasure(dimOrMeasure2Project: JHashMap[String, Array[String]]): Unit = {
    this.dimOrMeasure2Project = dimOrMeasure2Project
  }


  // 没有postFilter,即所有filter必须下推，
  // 并且project都是原始列，project不能使用任何函数,不能有任何类型转换
  // agg里也不能使用函数 org.apache.spark.sql.execution.datasources.DataSourceStrategy.translateAggregate
  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!scanJob.readOptions.pushDownAggregate) return false

    val dialect = JdbcDialects.get("jdbc:clickhouse")
    val compiledAgg = JDBCRDD.compileAggregates(aggregation.aggregateExpressions, dialect)
    if (compiledAgg.isEmpty) return false

    val groupByCols = aggregation.groupByColumns.map { col =>
      if (col.fieldNames.length != 1) return false
      quoted(col.fieldNames.head)
    }

    // The column names here are already quoted and can be used to build sql string directly.
    // e.g. "DEPT","NAME",MAX("SALARY"),MIN("BONUS") =>
    // SELECT "DEPT","NAME",MAX("SALARY"),MIN("BONUS") FROM "test"."employee"
    //   GROUP BY "DEPT", "NAME"
    val selectList = groupByCols ++ compiledAgg.get
    groupByClause = if (groupByCols.isEmpty) {
      ""
    } else {
      "GROUP BY " + groupByCols.mkString(",")
    }

    val aggQuery = s"SELECT ${selectList.mkString(",")} FROM ${database}.${table} " +
      s"WHERE 1=0 $groupByClause"
    try {
      _readSchema = Using.resource(GrpcNodeClient(scanJob.node)) { implicit grpcNodeClient: GrpcNodeClient =>
        getQueryOutputSchema(aggQuery, dialect)
      }
      pushedAggregateList = selectList
      pushedGroupByCols = Some(groupByCols)
      true
    } catch {
      case NonFatal(e) =>
        log.error("Failed to push down aggregation to CLICKHOUSE", e)
        false
    }
  }

  override def build(): Scan = new ClickHouseBatchScan(scanJob.copy(
    readSchema = _readSchema,
    pushedFilters = _pushedFilters,
    pushedAggregateColumn = pushedAggregateList,
    groupByColumns = pushedGroupByCols,
    filterExpr = filterWhereClause(AlwaysTrue :: pushedFilters.toList),
    groupByClause = groupByClause,
    dimOrMeasure2Project = dimOrMeasure2Project
  ))

}

// 原生的scan只能是先从数据源将明细数据拿到spark,然后在spark做各种函数的处理。
//  这种符合olap模型的查询，明细查询会被sqlParse转换成ck聚合查询
//    sqlParse 如果能匹配到模型则改写成聚合查询，否则表示没有对应模型，不需要改写，返回原始sql，查询初原始数据在spark处理各种函数
//    存在问题：如果有非聚合函数的处理，需要在模型定义一个衍生维度，对衍生维度查询，这样会把衍生维度在底层数据源处理，不定义衍生维度当做groupBy where 会造成数据错误
//    eg: select length(name),uv from table where length(age) > 10
//    期望 ---> select length(name),uv from table where length(age) > 10  group by length(name)
//    实际 ---> select name,uv from table group by name ,因为filter没法下推，project的函数也没办法下推
// with SupportsPushDownAggregates 改造后
//  对于 select m as c,value as cid, count(value) from clickhouse.dw_bigdata_olap.fact_table  group by c,cid
//  这种 没有使用函数的才会下推，只要有一个使用函数或者有 postFilter 都无法下推，都是查明细在spark处理,如下：
//  eg: select m as c,value as cid, count(length(value)) from clickhouse.dw_bigdata_olap.fact_table  group by c,cid
class ClickHouseBatchScan(scanJob: ScanJobDescription) extends Scan with Batch
  with SupportsReportPartitioning
  with PartitionReaderFactory
  with ClickHouseHelper {

  val database: String = scanJob.tableEngineSpec match {
    case dist: DistributedEngineSpec if scanJob.readOptions.convertDistributedToLocal => dist.local_db
    case _ => scanJob.tableSpec.database
  }

  val table: String = scanJob.tableEngineSpec match {
    case dist: DistributedEngineSpec if scanJob.readOptions.convertDistributedToLocal => dist.local_table
    case _ => scanJob.tableSpec.name
  }

  lazy val inputPartitions: Array[ClickHouseInputPartition] = scanJob.tableEngineSpec match {
    case DistributedEngineSpec(_, _, local_db, local_table, _, _) if scanJob.readOptions.convertDistributedToLocal =>
      scanJob.cluster.get.shards.flatMap { shardSpec =>
        Using.resource(GrpcNodeClient(shardSpec.nodes.head)) { implicit grpcNodeClient: GrpcNodeClient =>
          queryPartitionSpec(local_db, local_table).map(partitionSpec =>
            ClickHouseInputPartition(scanJob.localTableSpec.get, partitionSpec, shardSpec) // TODO pickup preferred
          )
        }
      }
    case _: DistributedEngineSpec if scanJob.readOptions.useClusterNodesForDistributed =>
      throw ClickHouseClientException(
        s"${READ_DISTRIBUTED_USE_CLUSTER_NODES.key} is not supported yet."
      )
    case _: DistributedEngineSpec =>
      // we can not collect all partitions from single node, thus should treat table as no partitioned table
      Array(ClickHouseInputPartition(scanJob.tableSpec, NoPartitionSpec, scanJob.node))
    case _: TableEngineSpec =>
      Using.resource(GrpcNodeClient(scanJob.node)) { implicit grpcNodeClient: GrpcNodeClient =>
        queryPartitionSpec(database, table).map(partitionSpec =>
          ClickHouseInputPartition(scanJob.tableSpec, partitionSpec, scanJob.node) // TODO pickup preferred
        )
      }.toArray
  }

  override def toBatch: Batch = this

  // may contains meta columns
  override def readSchema(): StructType = scanJob.readSchema

  override def planInputPartitions: Array[InputPartition] = inputPartitions.toArray

  override def outputPartitioning(): Partitioning = ClickHousePartitioning(inputPartitions)

  override def createReaderFactory: PartitionReaderFactory = this

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new ClickHouseReader(scanJob, partition.asInstanceOf[ClickHouseInputPartition])

  override def supportColumnarReads(partition: InputPartition): Boolean = false

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    super.createColumnarReader(partition)


  override def description(): String = {
    val (aggString, groupByString) = if (scanJob.groupByColumns.nonEmpty) {
      val groupByColumnsLength = scanJob.groupByColumns.get.length
      (seqToString(scanJob.pushedAggregateColumn.drop(groupByColumnsLength)),
        seqToString(scanJob.pushedAggregateColumn.take(groupByColumnsLength)))
    } else {
      ("[]", "[]")
    }
    super.description() + ", prunedSchema: " + seqToString(scanJob.readSchema) +
      ", PushedFilters: " + seqToString(scanJob.pushedFilters) +
      ", PushedAggregates: " + aggString + ", PushedGroupBy: " + groupByString
  }

  private def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

}
