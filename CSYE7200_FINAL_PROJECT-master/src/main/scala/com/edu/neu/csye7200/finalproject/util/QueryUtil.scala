package com.edu.neu.csye7200.finalproject.util

import com.edu.neu.csye7200.finalproject.configure.FileConfig
import com.edu.neu.csye7200.finalproject.util.DataUtil.spark
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse}

object QueryUtil {
  lazy val spark = SparkSession
    .builder()
    .appName("MovieRecommondation")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  /** query movieid by information and information type json format
    *
    * @param df   moviedata dataframe
    * @param content      user's input content
    * @param selectedType user's select for type of content ( companies,keywords,names)
    * @return Array[(Int,String,String,String,Date,Double)] movidId,selectedType,title,tagline,release_date,popularity
    */
  def QueryMovieJson(df: DataFrame, content: String, selectedType: String) = {

    val colsList = List(col("id"), col(selectedType),col("title"),col("tagline"),col("release_date"),col("popularity"))
    DataClean(df.select(colsList: _*)).filter(_(5)!=null).
      map(row=>(row.getInt(0),parse(row.getString(1).replaceAll("'","\"")
      .replaceAll("\\\\xa0","")
      .replaceAll("\\\\","")),row.getString(2),row.getString(3),row.getDate(4),
      row.getDouble(5)))
      .map(x => (x._1, compact(x._2 \ "name"),x._3,x._4,x._5,x._6))
      .filter(x => x._2.contains(content)).collect
  }

  /**
    * Query movie info  String format
    * @param df   moviedata dataframe
    * @param content    user's input content
    * @param selectedType user's select for type of content ( companies,keywords,names)
    * @return   Array[(Int,String,String,String,Date,Double)] movidId,selectedType,title,tagline,release_date,popularity
    */
  def QueryMovieInfoNorm(df:DataFrame,content:String,selectedType:String)={
      val colsList= List(col("id"), col(selectedType), col("title"), col("tagline"), col("release_date"), col("popularity"))
      df.select(colsList: _*).rdd.filter(_(0)!= null).filter(_(1)!=null).map(row => (row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getDate(4),
        row.getDouble(5))).filter(x => x._2.contains(content)).collect
  }
  def QueryMovieIdByName(df:DataFrame,content:String)={
    val colsList= List(col("id"), col("title"))
    df.select(colsList: _*).rdd.filter(_(0)!= null).filter(_(1)!=null).map(row => (row.getInt(0), row.getString(1))).filter(x=>x._2.equals(content)).collect
  }

  /**
    * clean invalid json  data prepare for parse
    * @param df
    * @return Rdd[Row]
    */
  def DataClean(df:DataFrame)={
    df.rdd.filter(_(0)!= null).filter(_(1)!=null).filter(x=> (x.getString(1).contains("'"))).filter(x=> (x.getString(1).contains("'name'")))
      .filter(row=> !row.getString(1).takeRight(1).equals("'"))

  }

  /**
    * Query movieid by keywords
    *
    * @param keywords  dataframe of Keywords
    * @param df   movie Dataframe
    * @param content  User's input content
    * @return Array with (id, keywords,title,tagline,release_date,popularity)
    */
  def QueryOfKeywords(keywords:DataFrame, df: DataFrame, content: String) = {
    val ids = DataClean(keywords).map(row=>(row.getInt(0),parse(row.getString(1).replaceAll("'","\"").replaceAll("\\\\xa0","")
      .replaceAll("\\\\",""))))
      .map(x => (x._1, compact(x._2 \ "name")))
      .filter(x => x._2.contains(content)).collect.take(20)
    ids.flatMap(id => df.select("title", "tagline", "release_date", "popularity").where("id==" + id._1).rdd.map {
      line => (id._1, id._2, line.getString(0), line.getString(1), line.getDate(2), line.getDouble(3))
    }.collect)
  }

  /**
    * Query movieid by staff
    *
    * @param staff  dataframe of staff
    * @param df   movie Dataframe
    * @param content  User's input content
    * @param SelectedType query content in crew/cast
    * @return Array with (id, staff,title,tagline,release_date,popularity)
    */
  def QueryOfstaff(staff:DataFrame,df:DataFrame,content:String,SelectedType:String)={
    var  index=0
    SelectedType match{
      case "crew"=> index=1
      case "cast"=>index=0
    }
    val ids=DataClean(staff).map(row=>(row.getInt(2),parse(row.getString(index).replaceAll("None","null").replaceAll("'","\"")
      .replaceAll("\\\\xa0","").replaceAll("\\\\","")))).map(x => (x._1, compact(x._2 \ "name")))
      .filter(x => x._2.contains(content)).collect.take(20)
    ids.flatMap(id => df.select("title", "tagline", "release_date", "popularity").where("id==" + id._1).rdd.map {
      line => (id._1, id._2, line.getString(0), line.getString(1), line.getDate(2), line.getDouble(3))
    }.collect)
  }
  lazy val bookDF: DataFrame = spark.read.option("header", true).csv(FileConfig.bookRating)

  def searchByName(df:DataFrame,content:String): Array[(Int, String, String, String, Int, String, Float)] ={
    df.rdd.filter(_(0)!= null).filter(_(1)!=null).filter(_(2)!= null).filter(_(3)!=null).filter(_(4)!= null).filter(_(5)!=null).filter(_(6)!= null)
      .map(row => (row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4), row.getString(5), row.getFloat(6)))
      .filter(x=>x._2.trim.toLowerCase.equals(content.trim.toLowerCase)).collect}

  def searchByISBN(df:DataFrame,content:String): Array[(Int, String, String, String, Int, String, Float)] ={
    df.rdd.filter(_(0)!= null).filter(_(1)!=null).filter(_(2)!= null).filter(_(3)!=null).filter(_(4)!= null).filter(_(5)!=null).filter(_(6)!= null)
      .map(row => (row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4), row.getString(5), row.getFloat(6)))
      .filter(x=>x._6.toLowerCase.equals(content.toLowerCase)).collect
  }

  def searchByAuthor(df:DataFrame,content:String): Array[(Int, String, String, String, Int, String, Float)] ={
    df.rdd.filter(_(0)!= null).filter(_(1)!=null).filter(_(2)!= null).filter(_(3)!=null).filter(_(4)!= null).filter(_(5)!=null).filter(_(6)!= null)
      .map(row => (row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4), row.getString(5), row.getFloat(6))).sortBy(_._7, ascending = false)
      .filter(x=>x._3.trim.toLowerCase.equals(content.trim.toLowerCase)).collect
  }

  def searchByPublisher(df:DataFrame,content:String): Array[(Int, String, String, String, Int, String, Float)] ={
    df.rdd.filter(_(0)!= null).filter(_(1)!=null).filter(_(2)!= null).filter(_(3)!=null).filter(_(4)!= null).filter(_(5)!=null).filter(_(6)!= null)
      .map(row => (row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4), row.getString(5), row.getFloat(6))).sortBy(_._7, ascending = false)
      .filter(x=>x._4.trim.toLowerCase.equals(content.trim.toLowerCase)).collect
  }

}

