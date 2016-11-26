package io.druid.indexer.spark

import java.util

import io.druid.data.input.impl._
import io.druid.segment.indexing.DataSchema
import com.fasterxml.jackson.core.`type`.TypeReference
import io.druid.indexing.common.task.Task
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
  * Created by king on 11/26/16.
  */
object TestSparkHiveBatchIndexTask {
  import TestScalaBatchIndexTask._

  val partitions = new util.ArrayList[PartitionInfo]()
  partitions.add(new PartitionInfo("time", "201611230800"))
  val hiveSpec = new HiveSpec("src", partitions, null)

  // 确定dataSchema, spark hive采用 MapInputRowParser, dataframe转换为key-value
  /**
    * "dataSchema": {
    *   "dataSource": "hiveDataSource",
    *   "parser": {
    *     "type": "map",
    *     "parseSpec": {
    *       "format": "timeAndDims",
    *       "timestampSpec": {
    *         "column": "l_shipdate",
    *         "format": "yyyy-MM-dd"
    *       },
    *       "dimensionsSpec": {
    *         "dimensions": [ "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
    *                         "l_returnflag", "l_linestatus", "l_shipinstruct", "l_shipmode", "l_comment" ],
    *         "dimensionExclusions": [ "l_shipdate", "l_tax", "count", "l_quantity",
    *                                 "l_discount", "l_extendedprice", "l_commitdate", "l_receiptdate" ],
    *         "spatialDimensions" : []
    *       }
    *     }
    *   },
    *   "metricsSpec": [
    *     { "type" : "count", "name" : "count" },
    *     { "type" : "longSum", "name": "L_QUANTITY_longSum", "fieldName": "l_quantity" },
    *     { "type" : "doubleSum", "name": "L_EXTENDEDPRICE_doubleSum", "fieldName": "l_extendedprice" },
    *     { "type" : "doubleSum", "name": "L_DISCOUNT_doubleSum", "fieldName": "l_discount" },
    *     { "type" : "doubleSum", "name": "L_TAX_doubleSum", "fieldName": "l_tax" }
    *   ],
    *   "granularitySpec": ...
    * }
    */
  val parserSpec = new TimeAndDimsParseSpec(
    new TimestampSpec("l_shipdate", "yyyy-MM-dd", null),
    new DimensionsSpec(
      seqAsJavaList(
        Seq(
          "l_orderkey",
          "l_partkey",
          "l_suppkey",
          "l_linenumber",
          "l_returnflag",
          "l_linestatus",
          "l_shipinstruct",
          "l_shipmode",
          "l_comment"
        ).map(new StringDimensionSchema(_))
      ),
      seqAsJavaList(
        Seq(
          "l_shipdate",
          "l_tax",
          "count",
          "l_quantity",
          "l_discount",
          "l_extendedprice",
          "l_commitdate",
          "l_receiptdate"
        )
      ),
      null
    )
  )

  val hiveDataSchema = new DataSchema(
    "hiveDataSource",
    objectMapper.convertValue(new MapInputRowParser(parserSpec), new TypeReference[java.util.Map[String, Any]]() {}),
    aggFactories.toArray, granSpec, objectMapper
  )

  def buildSparkHiveBatchIndexTask(): SparkBatchIndexTask =
    new SparkBatchIndexTask(
      taskId,
      hiveDataSchema,
      Seq(interval),
      null,
      hiveSpec,
      rowsPerPartition,
      rowsPerFlush,
      properties,
      master,
      new util.HashMap[String, Object](),
      indexSpec,
      classpathPrefix,
      hadoopDependencyCoordinates
    )
}


class TestSparkHiveBatchIndexTask extends FlatSpec with Matchers {

  import TestSparkHiveBatchIndexTask._
  import TestScalaBatchIndexTask._

  it should "properly deserialize" in {
    val taskPre = buildSparkHiveBatchIndexTask()
    val taskPost = objectMapper.readValue(objectMapper.writeValueAsString(taskPre), classOf[SparkBatchIndexTask])
    val task: Task = objectMapper.readValue(getClass.getResource("/spark_hive_index_spec.json"), classOf[Task])

    task.asInstanceOf[SparkBatchIndexTask].getDataSchema.getParser.getParseSpec should ===(taskPre.getDataSchema.getParser.getParseSpec)
    task.asInstanceOf[SparkBatchIndexTask].getHadoopDependencyCoordinates.asScala should ===(Seq("org.apache.spark:spark-hive_2.11:2.0.2-mmx0"))
  }


}
