package csye7200FinalProject.Schema

import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

object BookSchema {


  val bookSchema = StructType(
    Seq(
      StructField("Id", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Authors", StringType, true),
      StructField("ISBN", StringType, true),
      StructField("Rating", DoubleType, true),
      StructField("PublishYear", IntegerType, true),
      StructField("PublishMonth", IntegerType, true),
      StructField("PublishDay", IntegerType, true),
      StructField("Publisher", StringType, true),
      StructField("RatingDist5", StringType, true),
      StructField("RatingDist4", StringType, true),
      StructField("RatingDist3", StringType, true),
      StructField("RatingDist2", StringType, true),
      StructField("RatingDist1", StringType, true),
      StructField("RatingDistTotal", StringType, true),
      StructField("CountsOfReview", IntegerType, true),
      StructField("Language", StringType, true),
      StructField("pagesNumber", IntegerType, true),
      StructField("Description", StringType, true),
      StructField("Count of text reviews", IntegerType, true),

    )
  )
  val linkdataSchema = StructType(
    Seq(
      StructField("bookId", IntegerType, false),
      StructField("goodId", StringType, false),
      StructField("greeksId", IntegerType, false)
    )

  )
  val keywordsSchema = StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("keywords", StringType, true)
    )
  )

}
