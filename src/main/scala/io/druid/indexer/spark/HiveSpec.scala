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
               partitions: util.List[String],
               @JsonProperty("sql")
               sql: String) {

  @JsonProperty("table")
  def getTable = table

  @JsonProperty("partitions")
  def getPartitions = partitions

  @JsonProperty("sql")
  def getSql = sql
}