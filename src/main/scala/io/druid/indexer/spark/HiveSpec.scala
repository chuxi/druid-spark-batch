package io.druid.indexer.spark

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import java.util

/**
  * Created by king on 11/25/16.
  */
@JsonCreator
class HiveSpec(@JsonProperty("table")
               table: String,
               @JsonProperty("partitions")
               partitions: util.List[PartitionInfo],
               @JsonProperty("columns")
               columns: java.util.List[String],
               @JsonProperty("sql")
               sql: String) {

  if (table == null) {
    throw new Exception("Missing table definition in HiveSpec!")
  }

  @JsonProperty("table")
  def getTable = table

  @JsonProperty("partitions")
  def getPartitions = partitions

  @JsonProperty("columns")
  def getColumns = columns

  @JsonProperty("sql")
  def getSql = sql

}


@JsonCreator
class PartitionInfo(@JsonProperty("pkey") pkey: String,
                    @JsonProperty("pvalue") pvalue: String) {

  @JsonProperty("pkey")
  def getPkey = pkey

  @JsonProperty("pvalue")
  def getPvalue = pvalue

}