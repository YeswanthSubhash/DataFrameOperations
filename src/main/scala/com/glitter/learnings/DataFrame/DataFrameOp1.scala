package com.glitter.learnings.DataFrame

import org.apache.spark.sql._
object DataFrameOp1 extends session {

  def main(args:Array[String])={

    //Create a Data Frame from reading csv file
    val dfTags=spark
      .read
      .option("header","true")
      .option("inferSchema","true")
      .csv("file:///D://spark performance//question_tags.csv")
      .toDF()
    import spark.implicits._

    dfTags.show(10)

    //Print DataFrame Schema
    dfTags.printSchema()

    //Query dataframe: select columns from a dataframe
    dfTags
      .select("id","tag")
      .show(5)

    // DataFrame Query: filter by column value of a dataframe
    dfTags
        .filter("tag=='php'")
        .show(5)

    dfTags
      .filter($"tag"==="php")
      .show(5)

   //Count of php tags
    val counts =dfTags.filter("tag == 'php'").count()
    println(s"Number of php tags = ${ counts }")

    //DataFrame Query: SQL LIKE operator
    val tagsWithS=dfTags.filter("tag Like 's%'")
    tagsWithS.show(5)


    //DataFrame Query: SQL multi filter chain
    val multFilterChain=dfTags
      .filter("tag Like 's%'")
      .filter("id=25 or id=108")

    multFilterChain.show(5)

    //DataFrame Query: SQL IN clause
    val inclause=dfTags
      .filter("id in (23,108)")
    inclause.show(5)

    //DataFrame Query: SQL group by
    val groupbyclause=dfTags
      .groupBy("tag")
      .count()

    groupbyclause.show(6)

    //DataFrame Query: SQL group by with filter
    val groupbyfiltercount=dfTags
      .groupBy("tag")
      .count()
      .filter("count > 5")

    groupbyfiltercount.show(10)

    //DataFrame Query: SQL order by
    val orderbycount=dfTags
      .groupBy("tag")
      .count()
      .filter("count > 5")
      .orderBy("tag")

    orderbycount.show(10)







  }

}
