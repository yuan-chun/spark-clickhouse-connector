package xenon.clickhouse.read;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;

import java.util.HashMap;
import java.util.List;

@Evolving
public interface SupportPushDimAndMeasure extends SupportsPushDownRequiredColumns {


    void pushDimAndMeasure(HashMap<String, String[]> dimOrMeasure2Project);
}