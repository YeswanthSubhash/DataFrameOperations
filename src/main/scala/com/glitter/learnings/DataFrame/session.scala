package com.glitter.learnings.DataFrame

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait session {

  lazy val sparkConf=new SparkConf()
    .setAppName("sparkLearn")
    .setMaster("local[*]")

  lazy val spark=SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

}
