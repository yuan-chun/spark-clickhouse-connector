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

object SparkSQLClickHouse {

  // $example on:create_ds$
  case class Person(name: String, age: Long)

  // $example off:create_ds$

  def testPartition(spark: SparkSession): Unit = {
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


  def testNoPartition(spark: SparkSession): Unit = {
    val str = spark.conf.get("spark.some.config.option")

    println(str)
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=default")
    spark.sql("use clickhouse")
    spark.sql("select * from clickhouse.olap_znyy.olap_task_local limit 10").show()
    spark.sql("CREATE DATABASE IF NOT EXISTS olap ").show()
    spark.sql("show databases ").show()
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS tbl_1 (
         |  create_time TIMESTAMP NOT NULL,
         |        m           INT       NOT NULL COMMENT 'part key',
         |        id          BIGINT    NOT NULL COMMENT 'sort key',
         |        value       STRING
         |      ) USING clickhouse
         |      TBLPROPERTIES (
         |        cluster = 'default',
         |        engine = 'MergeTree()',
         |        order_by = '(id)')
         |        """.stripMargin)
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS
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

  def testQueryPlan(spark: SparkSession): Unit = {
    val str = spark.conf.get("spark.some.config.option")

    println(str)
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=default")
    spark.sql("use clickhouse")
    spark.sql("CREATE DATABASE IF NOT EXISTS olap ").show()
    spark.sql("show databases ").show()
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS
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
    //      query model
    val margin =
      s"""select create_time,m,id,length(value) as len,uv,pv from clickhouse.olap.fact_table
         | where id > 0 and length(value) > 1 and m > 1 or value is not null
         | order by m desc""".stripMargin
    val execution = spark.sql(margin).queryExecution
    spark.sql(margin).show()
    println("analyzed")
    println(execution.analyzed)
    println("optimizedPlan")
    println(execution.optimizedPlan)
    println("executedPlan")
    println(execution.executedPlan)
    println("explain")
    println(spark.sql(margin).explain().toString)

  }


  def t1QueryPlan(spark: SparkSession): Unit = {
    //    spark.experimental.extraOptimizations =
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=dw_bigdata_olap")
    spark.sql("set spark.clickhouse.read.pushDownAggregate=true")
    spark.sql("set spark.clickhouse.distributed.ddl.enable=false")
    //    spark.sql("use clickhouse")
    //    spark.sql("CREATE DATABASE IF NOT EXISTS dw_bigdata_olap ").show()
    //    spark.sql("show databases ").show()
    //    spark.sql(
    //      s"""CREATE TABLE IF NOT EXISTS
    //         |        clickhouse.dw_bigdata_olap.fact_table (
    //         |        create_time TIMESTAMP NOT NULL,
    //         |        m           INT       NOT NULL COMMENT 'part key',
    //         |        id          BIGINT    NOT NULL COMMENT 'sort key',
    //         |        value       STRING,
    //         |        uv          BIGINT    NOT NULL COMMENT 'uv',
    //         |        pv          BIGINT    NOT NULL COMMENT 'pv'
    //         |      ) USING clickhouse
    //         |      TBLPROPERTIES (
    //         |        cluster = 'default',
    //         |        engine = 'MergeTree()',
    //         |        order_by = '(id)')
    //         |        """.stripMargin)
    val margin = s"""select length(value),m from clickhouse.dw_bigdata_olap.fact_table where length(m) > 10 """
    //    val margin = s"""select m as c,value as cid, count((value)) from clickhouse.dw_bigdata_olap.fact_table  group by c,cid """
    val execution = spark.sql(margin).queryExecution
    spark.sql(margin).show()
    println("analyzed")
    println(execution.analyzed)
    println("optimizedPlan")
    println(execution.optimizedPlan)
    println("executedPlan")
    println(execution.executedPlan)
    println("explain")
    println(spark.sql(margin).explain().toString)
  }


  def testNestModel(spark: SparkSession): Unit = {
    //    spark.experimental.extraOptimizations =
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=dw_bigdata_olap")
    spark.sql("set spark.clickhouse.read.pushDownAggregate=true")
    spark.sql("set spark.clickhouse.distributed.ddl.enable=false")
//    val margin =
//      s"""
//         |select * from (
//         |SELECT  elapsed as a,length(trace_id) as b,create_time,countAll FROM  clickhouse.olap_znyy.olap_query_log_all_route where create_time > 10 and length(trace_id) > 1
//         |) where a > 0 order by a
//         | """.stripMargin
        val margin = s"""SELECT  elapsed as a,length(trace_id) as b,create_time,countAll FROM  clickhouse.olap_znyy.olap_query_log_all_route where create_time > 10 and length(trace_id) > 1"""
//        val margin = s"""select m as c,length(value),count(value) from clickhouse.dw_bigdata_olap.fact_table  group by c,value"""
    val execution = spark.sql(margin).queryExecution
    val frame = spark.sql(margin)
    println(s"schema ${frame.schema}")
    frame.show()
    frame.collect().foreach(println)
    println("analyzed")
    println(execution.analyzed)
    println("optimizedPlan")
    println(execution.optimizedPlan)
    println("executedPlan")
    println(execution.executedPlan)
    println("explain")
    println(spark.sql(margin).explain().toString)
  }


  def testRule(spark: SparkSession): Unit = {
    //    spark.experimental.extraOptimizations =
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=dw_bigdata_olap")
    spark.sql("set spark.clickhouse.read.pushDownAggregate=true")
    spark.sql("set spark.clickhouse.distributed.ddl.enable=false")
    //    spark.sql("set spark.sql.codegen.wholeStage=false")
    //    spark.sql("use clickhouse")
    //    spark.sql("CREATE DATABASE IF NOT EXISTS dw_bigdata_olap ").show()
    //    spark.sql("show databases ").show()
    //    spark.sql(
    //      s"""CREATE TABLE IF NOT EXISTS
    //         |        clickhouse.dw_bigdata_olap.fact_table (
    //         |        create_time TIMESTAMP NOT NULL,
    //         |        m           INT       NOT NULL COMMENT 'part key',
    //         |        id          BIGINT    NOT NULL COMMENT 'sort key',
    //         |        value       STRING,
    //         |        uv          BIGINT    NOT NULL COMMENT 'uv',
    //         |        pv          BIGINT    NOT NULL COMMENT 'pv'
    //         |      ) USING clickhouse
    //         |      TBLPROPERTIES (
    //         |        cluster = 'default',
    //         |        engine = 'MergeTree()',
    //         |        order_by = '(id)')
    //         |        """.stripMargin)
    //    val margin = s"""select value as bbb,m from clickhouse.dw_bigdata_olap.fact_table where value > 'aaa' and id > 10 """
    //    val margin = s"""select value,sum(m) from clickhouse.dw_bigdata_olap.fact_table where  length(value) > 10 group by value having sum(uv) > 1 """
        val margin = s"""select length(value),m as c,subString(value,1,6),value,pv,uv  from clickhouse.dw_bigdata_olap.fact_table where length(value) > 10 or value > 'aaa' and id > 10 """
//        val margin = s"""select m as c,value as cid, count((value)) from clickhouse.dw_bigdata_olap.fact_table  group by c,cid """
    val execution = spark.sql(margin).queryExecution
    //    println(execution.optimizedPlan)
    //    println(s"schema ${spark.sql(margin).schema}")
    val frame = spark.sql(margin)
    println(s"schema ${frame.schema}")
    frame.show()
    frame.collect().foreach(println)
    println("analyzed")
    println(execution.analyzed)
    println("optimizedPlan")
    println(execution.optimizedPlan)
    println("executedPlan")
    println(execution.executedPlan)
    println("explain")
    println(spark.sql(margin).explain().toString)
  }

  def testDetail(spark: SparkSession): Unit = {
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=dw_bigdata_olap")
    spark.sql("set spark.clickhouse.read.pushDownAggregate=true")
//    val margin = s"""select m as c,id,value,length(value)  from clickhouse.dw_bigdata_olap.fact_table where m > 0 and length(value) > 1 """
    val margin = s"""select value,uv from clickhouse.dw_bigdata_olap.fact_table where m > 10 and id > 10  """
    val execution = spark.sql(margin).queryExecution
    spark.sql(margin).show()
    spark.sql(margin).collect().foreach(println)
    println("analyzed")
    println(execution.analyzed)
    println("optimizedPlan")
    println(execution.optimizedPlan)
    println("executedPlan")
    println(execution.executedPlan)
    println("explain")
    println(spark.sql(margin).explain().toString)
  }

  def testPushAGG(spark: SparkSession): Unit = {
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=dw_bigdata_olap")
    spark.sql("set spark.clickhouse.read.pushDownAggregate=true")
    //    spark.sql("use clickhouse")
    //    spark.sql("CREATE DATABASE IF NOT EXISTS dw_bigdata_olap ").show()
    //    spark.sql("show databases ").show()
    //    spark.sql(
    //      s"""CREATE TABLE IF NOT EXISTS
    //         |        clickhouse.dw_bigdata_olap.table_push_agg (
    //         |        create_time TIMESTAMP NOT NULL,
    //         |        m           INT       NOT NULL COMMENT 'part key',
    //         |        id          BIGINT    NOT NULL COMMENT 'sort key',
    //         |        value       STRING,
    //         |        uv          BIGINT    NOT NULL COMMENT 'uv',
    //         |        pv          BIGINT    NOT NULL COMMENT 'pv'
    //         |      ) USING clickhouse
    //         |      TBLPROPERTIES (
    //         |        cluster = 'default',
    //         |        engine = 'MergeTree()',
    //         |        order_by = '(id)')
    //         |        """.stripMargin)
//    val margin =
//    s"""
//       |select c,cid,sub,id,cou,ma from
//       |(
//       |select m as c,length(value) as cid,subString(value,1,6) as sub ,id, count(uv) as cou,max(pv) as ma from clickhouse.dw_bigdata_olap.fact_table where m > 0 and length(value) > 1 group by id,m,length(value),subString(value,1,6)
//       |)
//       | """.stripMargin
        val margin = s"""select m as c,length(value) as cid,subString(value,1,6),id, count(uv),max(pv)  from clickhouse.dw_bigdata_olap.fact_table where m > 0 and length(value) > 1 group by id,m,length(value),subString(value,1,6) """
//        val margin = s"""select m as c,id, count(uv),max(pv)  from clickhouse.dw_bigdata_olap.fact_table where m > 0 and length(value) > 1 group by id,m"""
    val execution = spark.sql(margin).queryExecution
    spark.sql(margin).show()
    spark.sql(margin).collect().foreach(println)
    println("analyzed")
    println(execution.analyzed)
    println("optimizedPlan")
    println(execution.optimizedPlan)
    println("executedPlan")
    println(execution.executedPlan)
    println("explain")
    println(spark.sql(margin).explain().toString)
  }

  def testDDL(spark: SparkSession): Unit = {
    spark.sql("set spark.sql.catalog.clickhouse=xenon.clickhouse.ClickHouseCatalog")
    spark.sql("set spark.sql.catalog.clickhouse.host=localhost")
    spark.sql("set spark.sql.catalog.clickhouse.grpc_port=9111")
    spark.sql("set spark.sql.catalog.clickhouse.user=default")
    spark.sql("set spark.sql.catalog.clickhouse.password=default")
    spark.sql("set spark.sql.catalog.clickhouse.database=dw_bigdata_olap")
    spark.sql("set spark.clickhouse.read.pushDownAggregate=false")
    spark.sql("set spark.clickhouse.distributed.ddl.enable=true")
    spark.sql("use clickhouse")
    spark.sql("CREATE DATABASE IF NOT EXISTS dw_bigdata_olap ").show()
    spark.sql("show databases ").show()
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS
         |        clickhouse.dw_bigdata_olap.table_ddl (
         |        create_time TIMESTAMP NOT NULL,
         |        m           INT       NOT NULL COMMENT 'part key',
         |        id          BIGINT    NOT NULL COMMENT 'sort key',
         |        value       STRING,
         |        uv          BIGINT    NOT NULL COMMENT 'uv',
         |        pv          BIGINT    NOT NULL COMMENT 'pv'
         |      ) USING clickhouse
         |      TBLPROPERTIES (
         |        cluster = 'default',
         |        engine = 'MergeTree()',
         |        order_by = '(id)')
         |        """.stripMargin)
    spark.sql("ALTER TABLE clickhouse.dw_bigdata_olap.table_ddl  ADD COLUMN addCol1 bigint COMMENT 'uv'").show()
  }

  def testJDBC(spark: SparkSession): Unit = {
    val str = spark.conf.get("spark.some.config.option")
    println(str)
    new JDBCTableCatalog
    spark.sql("set spark.sql.catalog.mysql=org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
    spark.sql("set spark.sql.catalog.mysql.url=jdbc:mysql://localhost:3306/olap?user=root&password=12345678")
    spark.sql("set spark.sql.catalog.mysql.driver=com.mysql.cj.jdbc.Driver")
    spark.sql("set spark.sql.catalog.mysql.pushDownAggregate=true")
    spark.sql("set spark.sql.catalog.mysql.dbtable=olap_task")
    spark.sql("use mysql")
    spark.sql("show databases ").show()
    spark.sql("show tables ").show()
    val margin = s"""select name,count(1),max(id) from olap_task where id > 10 group by name order by name limit 10""".stripMargin
    //    spark.sql(
    //      s"""INSERT INTO `binlog_1` (id,`name`,`type`,addr,addCol,addCol1)
    //         | VALUES (18,'insert',null,'addr',1,'addr')""".stripMargin)
    //    spark.sql("ALTER TABLE binlog_1  ADD COLUMN addCol1 String COMMENT 'uv'").show()
    //    spark.sql("select * from binlog_1").show()
    //    spark.sql("show create table binlog_1").show(10000)
    val execution = spark.sql(margin).queryExecution
    spark.sql(margin).show()
    println("analyzed")
    println(execution.analyzed)
    println("optimizedPlan")
    println(execution.optimizedPlan)
    println("executedPlan")
    println(execution.executedPlan)
    println("explain")
    println(spark.sql(margin).explain().toString)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL clickhouse")
      //      .config("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown")
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.sql.extensions", "org.apache.spark.sql.OlapExtension")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //    testPartition(spark)
    //    testNoPartition(spark)
    //    testQueryPlan(spark)
    //    t1QueryPlan(spark)
//        testRule(spark)
//        testNestModel(spark)
        testPushAGG(spark)
        testDetail(spark)
    //    testDDL(spark)
    //    testJDBC(spark)
    spark.stop()
  }
}
