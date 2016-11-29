package io.druid.indexer.spark

import org.scalatest.{FlatSpec, Matchers}
import java.io.{Closeable, File}
import java.nio.file.Files

import com.google.common.io.Closer
import com.metamx.common.{CompressionUtils, Granularity}
import io.druid.common.utils.JodaUtils
import io.druid.segment.QueryableIndexIndexableAdapter
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
/**
  * Created by king on 11/26/16.
  */
class TestSparkHiveDruidIndexer extends FlatSpec with Matchers {
  import TestScalaBatchIndexTask._
  import TestSparkHiveBatchIndexTask._

  "The spark indexer" should "return proper DataSegments from hive" in {
    // 1. 准备本地测试数据
    val warehouse = "/tmp/spark-warehouse"
    val closer = Closer.create()
    val outDir = Files.createTempDirectory("segments").toFile
    (outDir.mkdirs() || outDir.exists()) && outDir.isDirectory should be(true)
    closer.register(
      new Closeable()
      {
        override def close(): Unit = FileUtils.deleteDirectory(outDir)
      }
    )
    try {
      val conf = new SparkConf()
        .setAppName("Simple Application")
        .setMaster("local[4]")
        .set("user.timezone", "UTC")
        .set("file.encoding", "UTF-8")
        .set("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
        .set("org.jboss.logging.provider", "slf4j")
        .set("druid.processing.columnCache.sizeBytes", "1000000000")
        .set("spark.driver.host", "localhost")
        .set("spark.executor.userClassPathFirst", "true")
        .set("spark.driver.userClassPathFirst", "true")
        .set("spark.kryo.referenceTracking", "false")
        .set("spark.sql.warehouse.dir", warehouse)
        .registerKryoClasses(SparkBatchIndexTask.getKryoClasses())

      val sc = new SparkContext(conf)
      val session = new TestHiveContext(sc, loadTestTables = false).sparkSession
      // load file lineitem.small.tbl, the schema:
      //  orderkey, partkey, suppkey, linenumber, quantity,
      // extendedprice, discount, tax, returnflag, linestatus,
      // shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment

      session.sql(
        """
          |create table src(
          |   l_orderkey int,
          |   l_partkey int,
          |   l_suppkey int,
          |   l_linenumber int,
          |   l_quantity int,
          |   l_extendedprice double,
          |   l_discount double,
          |   l_tax double,
          |   l_returnflag string,
          |   l_linestatus string,
          |   l_shipdate string,
          |   l_commitdate string,
          |   l_receiptdate string,
          |   l_shipinstruct string,
          |   l_shipmode string,
          |   l_comment string
          |)
          |partitioned by (time string)
          |ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
          |STORED AS TEXTFILE
          |TBLPROPERTIES ('serialization.null.format'='')
        """.stripMargin)

      def quoteHiveFile(p: String): String = {
        Thread.currentThread().getContextClassLoader.getResource(p).getPath
      }

      for (time <- Seq("201611230800", "201611230900")) {
        session.sql(
          s"""
             |LOAD DATA LOCAL INPATH '${quoteHiveFile("lineitem.small.tbl")}'
             |OVERWRITE INTO TABLE src PARTITION (time = '$time')
          """.stripMargin)
      }
      closer.register(
        new Closeable
        {
          override def close(): Unit = session.stop()
        }
      )

      //      val sql =
      //        """
      //          |select
      //          | l_shipdate, l_orderkey, l_partkey, l_suppkey, l_linenumber, l_tax, l_quantity, l_discount,
      //          | l_returnflag, l_linestatus, l_shipinstruct, l_shipmode, l_comment
      //          |from src where time = '201611230800'
      //        """.stripMargin
      //      session.sql(sql).show()


      val loadResults = SparkDruidIndexer.loadData(
        Seq(),
        hiveSpec,
        new SerializedJson(hiveDataSchema),
        SparkBatchIndexTask.mapToSegmentIntervals(Seq(interval), Granularity.YEAR),
        rowsPerPartition,
        rowsPerFlush,
        outDir.toString,
        indexSpec,
        sc
      )

      loadResults.length should be(7)

      for (
        segment <- loadResults
      ) {
        segment.getBinaryVersion should be(9)
        segment.getDataSource should equal(hiveDataSchema.getDataSource)
        interval.contains(segment.getInterval) should be(true)
        segment.getInterval.contains(interval) should be(false)
        segment.getSize should be > 0L
        segment.getDimensions.asScala.toSet should
          equal(
            hiveDataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet --
              hiveDataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionExclusions.asScala.toSet
          )
        segment.getMetrics.asScala.toList should equal(hiveDataSchema.getAggregators.map(_.getName).toList)
        val file = new File(segment.getLoadSpec.get("path").toString)
        file.exists() should be(true)
        val segDir = Files.createTempDirectory(outDir.toPath, "loadableSegment-%s" format segment.getIdentifier).toFile
        val copyResult = CompressionUtils.unzip(file, segDir)
        copyResult.size should be > 0L
        copyResult.getFiles.asScala.map(_.getName).toSet should equal(Set("00000.smoosh", "meta.smoosh", "version.bin"))
        val index = StaticIndex.INDEX_IO.loadIndex(segDir)
        try {
          val qindex = new QueryableIndexIndexableAdapter(index)
          qindex.getDimensionNames.asScala.toSet should
            equal(
              hiveDataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionNames.asScala.toSet --
                hiveDataSchema.getParser.getParseSpec.getDimensionsSpec.getDimensionExclusions.asScala.toSet
            )
          for (dimension <- qindex.getDimensionNames.iterator().asScala) {
            val dimVal = qindex.getDimValueLookup(dimension).asScala
            dimVal should not be 'Empty
            for (dv <- dimVal) {
              Option(dv) match {
                case Some(v) =>
                  dv should not be null
                  // I had a problem at one point where dimension values were being stored as lists
                  // This is a check to make sure the dimension is a list of values rather than being a list of lists
                  // If the unit test is ever modified to have dimension values that start with this offending case
                  // then of course this test will fail.
                  dv should not startWith "List("
                  dv should not startWith "Set("
                case None => //Ignore
              }
            }
          }
          qindex.getNumRows should be > 0
          for (colName <- Seq("count")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              for (i <- Range.apply(0, qindex.getNumRows)) {
                column.getLongSingleValueRow(i) should not be 0
              }
            }
            finally {
              column.close()
            }
          }
          for (colName <- Seq("L_QUANTITY_longSum")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              Range.apply(0, qindex.getNumRows).map(column.getLongSingleValueRow).sum should not be 0
            }
            finally {
              column.close()
            }
          }
          for (colName <- Seq("L_DISCOUNT_doubleSum", "L_TAX_doubleSum")) {
            val column = index.getColumn(colName).getGenericColumn
            try {
              Range.apply(0, qindex.getNumRows).map(column.getFloatSingleValueRow).sum should not be 0.0D
            }
            finally {
              column.close()
            }
          }
          index.getDataInterval.getEnd.getMillis should not be JodaUtils.MAX_INSTANT
        }
        finally {
          index.close()
        }
      }

    } finally {
      closer.close()
    }

  }
}
