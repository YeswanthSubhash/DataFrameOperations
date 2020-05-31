package com.glitter.learnings.DataFrame

object sparkCatalog extends App with session {

  val catalog = spark.catalog
  catalog.listDatabases().show(false)

  val dfTags = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("file:///D://spark performance//question_tags.csv")
    .toDF("id", "tag")


  //Create Temp View for the Dataframe
  val dfQuestionsCSV = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("file:///D://spark performance//questions.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")

  dfQuestionsCSV.createTempView("questionTempView")
  dfTags.createTempView("dfTagsTempView")

  catalog.listTables().show()

  //Caching the dataframe
  dfQuestionsCSV.cache()

  println("dfQuestionsCSV is Cached:"+catalog.isCached("questionTempView"))
  println("dfTags is Cached: "+catalog.isCached("dfTagsTempView"))

  //Drop Temp View using Catalog
  catalog.listTables().show()
  catalog.dropTempView("dfTagsTempView")
  catalog.listTables().show()

  //Listing the
  catalog.listFunctions().
    select("name","description","className","isTemporary").show(100,false)




}
