package com.glitter.learnings.DataFrame


import com.databricks.spark.xml._
object sparkXmlFromDf extends App with session {

  val dfQuestionsCSV = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("file:///D://spark performance//questions.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  //Add com.databricks.spark.xml in pom.xml file
  dfQuestionsCSV.write
    .format("com.databricks.spark.xml")
    .option("rootTag","Items")
    .option("rowTag","Item")
    .save("file:///D://spark performance//newQuestions.xml")
}
