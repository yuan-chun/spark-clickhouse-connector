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
package xenon.clickhouse.example

// $example on:programmatic_schema$
// $example off:programmatic_schema$
// $example on:init_session$
// import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
// $example off:data_types$
// $example off:programmatic_schema$

object SparkSQLClickHouse{

  // $example on:create_ds$
  case class Person(name: String, age: Long)
  // $example off:create_ds$

  def testPartition(spark: SparkSession) : Unit = {
    val str = spark.conf.get("spark.some.config.option")

    println(str)
    spark.sql("set spark.sql.catalog.clickhouse2=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse2.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse2.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse2.user=default")
    spark.sql("set spark.sql.catalog.clickhouse2.password=default")
    spark.sql("set spark.sql.catalog.clickhouse2.database=default")
    spark.sql("use clickhouse2")
    spark.sql("show databases ").show()
    spark.sql("select * from clickhouse2.default.tbl where id > 0 and m = 1 limit 2").show()
    println(
      spark.sql(
      s"""select max(id)
         | from clickhouse2.default.tbl
         | where id > 0 and m = 1
         | """.stripMargin).explain().toString
    )
//    spark.sql(s"""CREATE TABLE IF NOT EXISTS tbl (
//                 |  create_time TIMESTAMP NOT NULL,
//                 |        m           INT       NOT NULL COMMENT 'part key',
//                 |        id          BIGINT    NOT NULL COMMENT 'sort key',
//                 |        value       STRING
//                 |      ) USING clickhouse
//                 |PARTITIONED BY (m)
//                 |      TBLPROPERTIES (
//                 |        engine = 'MergeTree()',
//                 |        order_by = '(id)')
//                 |        """.stripMargin)
//    spark.sql("desc formatted clickhouse2.default.tbl ").show()
//    spark.sql(
//      s"""insert into clickhouse2.default.tbl values
//      |         (timestamp'2021-01-01 10:10:10', 1, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 2, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 3, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 4, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 5, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 6, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 7, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 8, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 9, 1L, '1'),
//      |         (timestamp'2021-01-01 10:10:10', 10, 1L, '1'),
//      |         (timestamp'2022-02-02 10:10:10', 11, 2L, '2')
//      |         as tabl(create_time, m, id, value);
//      |""".stripMargin).show()
//    spark.sql("select * from clickhouse2.default.tbl ").show()
//

  }



  def testNoPartition(spark: SparkSession) : Unit = {
      val str = spark.conf.get("spark.some.config.option")

      println(str)
      spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
      spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
      spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9011")
      spark.sql("set spark.sql.catalog.clickhouse.user=default")
      spark.sql("set spark.sql.catalog.clickhouse.password=default")
      spark.sql("set spark.sql.catalog.clickhouse.database=default")
      spark.sql("use clickhouse")
      spark.sql("select count(1) from clickhouse.olap_znyy.olap_task limit 10")
      spark.sql("show databases ").show()
      spark.sql(s"""CREATE TABLE IF NOT EXISTS tbl_1 (
                 |  create_time TIMESTAMP NOT NULL,
                 |        m           INT       NOT NULL COMMENT 'part key',
                 |        id          BIGINT    NOT NULL COMMENT 'sort key',
                 |        value       STRING
                 |      ) USING clickhouse
                 |      TBLPROPERTIES (
                 |        engine = 'MergeTree()',
                 |        order_by = '(id)')
                 |        """.stripMargin)
      spark.sql(s"""CREATE TABLE IF NOT EXISTS
                   |        clickhouse.olap.fact_table (
                   |        create_time TIMESTAMP NOT NULL,
                   |        m           INT       NOT NULL COMMENT 'part key',
                   |        id          BIGINT    NOT NULL COMMENT 'sort key',
                   |        value       STRING,
                   |        uv          BIGINT    NOT NULL COMMENT 'uv',
                   |        pv          BIGINT    NOT NULL COMMENT 'pv'
                   |      ) USING clickhouse
                   |      TBLPROPERTIES (
                   |        engine = 'MergeTree()',
                   |        order_by = '(id)')
                   |        """.stripMargin)
//        query table
//      val margin =
//         s"""select * from clickhouse.default.tbl_1
//           | where id > 0 and m > 1 or value is not null
//           | order by m desc
//           | limit 2 """.stripMargin

//      query model
      val margin =
        s"""select create_time,m,id,value,uv,pv from clickhouse.olap.fact_table
           | where id > 0 and m > 1 or value is not null
           | order by m desc
           | limit 2 """.stripMargin
      spark.sql(margin).show()
      println("analyzed")
      println(spark.sql(margin).queryExecution.analyzed)
      println("optimizedPlan")
      println(spark.sql(margin).queryExecution.optimizedPlan)
      println("executedPlan")
      println(spark.sql(margin).queryExecution.executedPlan)
      println("explain")
      println(spark.sql(margin).explain().toString)

//      spark.sql("desc formatted clickhouse.default.tbl_1 ").show()
//      spark.sql(
//        s"""insert into clickhouse.default.tbl_1 values
//        |         (timestamp'2021-01-01 10:10:10', 1, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 2, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 3, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 4, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 5, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 6, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 7, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 8, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 9, 1L, '1'),
//        |         (timestamp'2021-01-01 10:10:10', 10, 1L, '1'),
//        |         (timestamp'2022-02-02 10:10:10', 11, 2L, '2')
//        |         as tabl(create_time, m, id, value);
//        |""".stripMargin).show()
//      spark.sql("select * from clickhouse.default.tbl_1 ").show()


  }


  def testJDBC(spark: SparkSession) : Unit = {
    val str = spark.conf.get("spark.some.config.option")
    println(str)
    new JDBCTableCatalog
    spark.sql("set spark.sql.catalog.mysql" +
      "=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
    spark.sql("set spark.sql.catalog.mysql.url" +
      "=jdbc:mysql://localhost:3306/olap?user=root&password=12345678")
    spark.sql("set spark.sql.catalog.mysql.driver=com.mysql.cj.jdbc.Driver")
    spark.sql("set spark.sql.catalog.mysql.pushDownAggregate=true")
    spark.sql("set spark.sql.catalog.mysql.dbtable=olap_task")
    spark.sql("use mysql")
    spark.sql("show databases ").show()
    spark.sql("show tables ").show()
//    spark.sql(
//      s"""INSERT INTO `binlog_1` (id,`name`,`type`,addr,addCol,addCol1)
//         | VALUES (18,'insert',null,'addr',1,'addr')""".stripMargin)
//    spark.sql("ALTER TABLE binlog_1  ADD COLUMN addCol1 String COMMENT 'uv'").show()
//    spark.sql("select * from binlog_1").show()
//    spark.sql("show create table binlog_1").show(10000)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL clickhouse")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

//    testPartition(spark)
    testNoPartition(spark)
//    testJDBC(spark)
    spark.stop()
  }
}
