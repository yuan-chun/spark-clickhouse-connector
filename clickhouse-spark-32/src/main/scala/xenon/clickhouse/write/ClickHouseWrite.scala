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

package xenon.clickhouse.write

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write._
import xenon.clickhouse.write.WriteAction._

class ClickHouseWriteBuilder(writeJob: WriteJobDescription)
    extends WriteBuilder with SupportsTruncate {

  private var action: WriteAction = APPEND

  override def truncate(): WriteBuilder = {
    this.action = TRUNCATE
    this
  }

  override def build(): Write = new ClickHouseWrite(writeJob, action)
}

class ClickHouseWrite(
  writeJob: WriteJobDescription,
  action: WriteAction
) extends Write
    with RequiresDistributionAndOrdering
    with SQLConfHelper {

  override def requiredDistribution(): Distribution = Distributions.clustered(writeJob.sparkParts.toArray)

  override def requiredNumPartitions(): Int = conf.getConf(WRITE_REPARTITION_NUM)

  override def requiredOrdering(): Array[SortOrder] = writeJob.sparkSortOrders

  override def toBatch: BatchWrite = new ClickHouseBatchWrite(writeJob, action)
}

class ClickHouseBatchWrite(
  writeJob: WriteJobDescription,
  action: WriteAction
) extends BatchWrite with DataWriterFactory {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = this

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = action match {
    case APPEND =>
      new ClickHouseAppendWriter(writeJob)
    case TRUNCATE =>
      new ClickHouseTruncateWriter(writeJob)
  }
}
