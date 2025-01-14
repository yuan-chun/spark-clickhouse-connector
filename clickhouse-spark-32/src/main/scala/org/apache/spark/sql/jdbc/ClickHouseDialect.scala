package org.apache.spark.sql.jdbc

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, DeleteColumn, RenameColumn, UpdateColumnNullability, UpdateColumnType}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRDD, JdbcUtils}
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp, Types}
import java.util.Locale
import scala.collection.mutable.ArrayBuilder
import scala.util.matching.Regex

/**
 * ClickHouse SQL dialect
 */
object ClickHouseDialect extends JdbcDialect with Logging {

  private[jdbc] val arrayTypePattern: Regex = "^Array\\((.*)\\)$".r
  private[jdbc] val dateTypePattern: Regex = "^[dD][aA][tT][eE]$".r
  private[jdbc] val dateTimeTypePattern: Regex = "^[dD][aA][tT][eE][tT][iI][mM][eE](64)?(\\((.*)\\))?$".r
  private[jdbc] val decimalTypePattern: Regex = "^[dD][eE][cC][iI][mM][aA][lL]\\((\\d+),\\s*(\\d+)\\)$".r
  private[jdbc] val decimalTypePattern2: Regex = "^[dD][eE][cC][iI][mM][aA][lL](32|64|128|256)\\((\\d+)\\)$".r
  private[jdbc] val enumTypePattern: Regex = "^Enum(8|16)$".r
  private[jdbc] val fixedStringTypePattern: Regex = "^FixedString\\((\\d+)\\)$".r
  private[jdbc] val nullableTypePattern: Regex = "^Nullable\\((.*)\\)".r
  val url: String =  "jdbc:clickhouse"

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith(url)

  /**
   * Inferred schema always nullable.
   * see [[JDBCRDD.resolveTable]]
   */
  override def getCatalystType(sqlType: Int,
                               typeName: String,
                               size: Int,
                               md: MetadataBuilder): Option[DataType] = {
    val scale = md.build.getLong("scale").toInt
    logDebug(s"sqlType: $sqlType, typeName: $typeName, precision: $size, scale: $scale")
    sqlType match {
      case Types.ARRAY =>
        unwrapNullable(typeName) match {
          case (_, arrayTypePattern(nestType)) =>
            toCatalystType(nestType, size, scale).map { case (nullable, dataType) => ArrayType(dataType, nullable) }
          case _ => None
        }
      case _ => toCatalystType(typeName, size, scale).map(_._2)
    }
  }

  /**
   * Spark use a widening conversion both ways, see detail at
   * [[https://github.com/apache/spark/pull/26301#discussion_r347725332]]
   */
  private[jdbc] def toCatalystType(typeName: String,
                                   precision: Int,
                                   scale: Int): Option[(Boolean, DataType)] = {
    val (nullable, _typeName) = unwrapNullable(typeName)
    val dataType = _typeName match {
      case "String" | "UUID" | fixedStringTypePattern() | enumTypePattern(_) => Some(StringType)
      case "Int8" => Some(ByteType)
      case "UInt8" | "Int16" => Some(ShortType)
      case "UInt16" | "Int32" => Some(IntegerType)
      case "UInt32" | "Int64" | "UInt64" | "IPv4" => Some(LongType) // UInt64 is not fully support
      case "Int128" | "Int256" | "UInt256" => None // not support
      case "Float32" => Some(FloatType)
      case "Float64" => Some(DoubleType)
      case dateTypePattern() => Some(DateType)
      case dateTimeTypePattern() => Some(TimestampType)
      case decimalTypePattern(precision, scale) => Some(DecimalType(precision.toInt, scale.toInt))
      case decimalTypePattern2(w, scale) => w match {
        case "32" => Some(DecimalType(9, scale.toInt))
        case "64" => Some(DecimalType(18, scale.toInt))
        case "128" => Some(DecimalType(38, scale.toInt))
        case "256" => Some(DecimalType(76, scale.toInt)) // throw exception, spark support precision up to 38
      }
      case _ => None
    }
    dataType.map((nullable, _))
  }

  private[jdbc] def unwrapNullable(maybeNullableTypeName: String): (Boolean, String) = maybeNullableTypeName match {
    case nullableTypePattern(typeName) => (true, typeName)
    case _ => (false, maybeNullableTypeName)
  }

  /**
   * NOT recommend auto create ClickHouse table by Spark JDBC, the reason is it's hard to handle nullable because
   * ClickHouse use `T` to represent ANSI SQL `T NOT NULL` and `Nullable(T)` to represent ANSI SQL `T NULL`,
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("String", Types.VARCHAR))
    // ClickHouse doesn't have the concept of encodings. Strings can contain an arbitrary set of bytes,
    // which are stored and output as-is.
    // See detail at https://clickhouse.tech/docs/en/sql-reference/data-types/string/
    case BinaryType => Some(JdbcType("String", Types.BINARY))
    case BooleanType => Some(JdbcType("UInt8", Types.BOOLEAN))
    case ByteType => Some(JdbcType("Int8", Types.TINYINT))
    case ShortType => Some(JdbcType("Int16", Types.SMALLINT))
    case IntegerType => Some(JdbcType("Int32", Types.INTEGER))
    case LongType => Some(JdbcType("Int64", Types.BIGINT))
    case FloatType => Some(JdbcType("Float32", Types.FLOAT))
    case DoubleType => Some(JdbcType("Float64", Types.DOUBLE))
    case t: DecimalType => Some(JdbcType(s"Decimal(${t.precision},${t.scale})", Types.DECIMAL))
    case DateType => Some(JdbcType("Date", Types.DATE))
    case TimestampType => Some(JdbcType("DateTime", Types.TIMESTAMP))
    case ArrayType(et, _) if et.isInstanceOf[AtomicType] =>
      getJDBCType(et)
        .orElse(JdbcUtils.getCommonJDBCType(et))
        .map(jdbcType => JdbcType(s"Array(${jdbcType.databaseTypeDefinition})", Types.ARRAY))
    case _ => None
  }

  override def quoteIdentifier(colName: String): String = s"`$colName`"

  override def isCascadingTruncateTable: Option[Boolean] = Some(false)

  override def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString("[", ",", "]")
    case _ => value
  }


  override def alterTable(
                  tableName: String,
                  changes: Seq[TableChange],
                  dbMajorVersion: Int): Array[String] = {
    val updateClause = ArrayBuilder.make[String]
    for (change <- changes) {
      change match {
        case add: AddColumn if add.fieldNames.length == 1 =>
          val dataType = JdbcUtils.getJdbcType(add.dataType(), this).databaseTypeDefinition
          val name = add.fieldNames
          updateClause += getAddColumnQuery(tableName, name(0), dataType)
        case rename: RenameColumn if rename.fieldNames.length == 1 =>
          val name = rename.fieldNames
          updateClause += getRenameColumnQuery(tableName, name(0), rename.newName, dbMajorVersion)
        case delete: DeleteColumn if delete.fieldNames.length == 1 =>
          val name = delete.fieldNames
          updateClause += getDeleteColumnQuery(tableName, name(0))
        case updateColumnType: UpdateColumnType if updateColumnType.fieldNames.length == 1 =>
          val name = updateColumnType.fieldNames
          val dataType = JdbcUtils.getJdbcType(updateColumnType.newDataType(), this)
            .databaseTypeDefinition
          updateClause += getUpdateColumnTypeQuery(tableName, name(0), dataType)
        case updateNull: UpdateColumnNullability if updateNull.fieldNames.length == 1 =>
          val name = updateNull.fieldNames
          updateClause += getUpdateColumnNullabilityQuery(tableName, name(0), updateNull.nullable())
        case _ =>
          throw QueryCompilationErrors.unsupportedTableChangeInJDBCCatalogError(change)
      }
    }
    updateClause.result()
  }

  override def getAddColumnQuery(tableName: String, columnName: String, dataType: String): String =
    s"ALTER TABLE $tableName ADD COLUMN ${quoteIdentifier(columnName)} $dataType"

  override def getRenameColumnQuery(
                            tableName: String,
                            columnName: String,
                            newName: String,
                            dbMajorVersion: Int): String =
    s"ALTER TABLE $tableName RENAME COLUMN ${quoteIdentifier(columnName)} TO" +
      s" ${quoteIdentifier(newName)}"

  override def getDeleteColumnQuery(tableName: String, columnName: String): String =
    s"ALTER TABLE $tableName DROP COLUMN ${quoteIdentifier(columnName)}"

  override def getUpdateColumnTypeQuery(
                                tableName: String,
                                columnName: String,
                                newDataType: String): String =
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} $newDataType"

  override def getUpdateColumnNullabilityQuery(
                                       tableName: String,
                                       columnName: String,
                                       isNullable: Boolean): String = {
    val nullable = if (isNullable) "NULL" else "NOT NULL"
    s"ALTER TABLE $tableName ALTER COLUMN ${quoteIdentifier(columnName)} SET $nullable"
  }
}
