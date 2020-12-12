package csye7200FinalProject.Collections

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.json4s.jackson.JsonMethods.{compact, parse}
import org.json4s

object QueryUtil {

 
  def QueryBookJson(df: DataFrame, content: String, selectedType: String) = {

    val colsList = List(col("Id"), col(selectedType), col("Name"), col("Description"), col("PublishYear"), col("RatingDistTotal"))
    DataClean(df.select(colsList: _*)).filter(_ (5) != null).
      map(row => (row.getInt(0), parse(row.getString(1).replaceAll("'", "\"")
        .replaceAll("\\\\xa0", "")
        .replaceAll("\\\\", "")), row.getString(2), row.getString(3), row.getInt(4),
        row.getString(5)))
      .map(x => (x._1, compact(x._2 \ "name"), x._3, x._4, x._5, x._6))
      .filter(x => x._2.contains(content)).collect
  }

 
  def QueryBookInfoNorm(df: DataFrame, content: String, selectedType: String) = {
    val colsList = List(col("Id"), col(selectedType), col("Name"), col("Description"), col("PublishYear"), col("RatingDistTotal"))
    df.select(colsList: _*).rdd.filter(_ (0) != null).filter(_ (1) != null).map(row => (row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4),
      row.getString(5))).filter(x => x._2.contains(content)).collect
  }

  def QueryBookIdByName(df: DataFrame, content: String) = {
    val colsList = List(col("Id"), col("Name"))
    df.select(colsList: _*).rdd.filter(_ (0) != null).filter(_ (1) != null).map(row => (row.getInt(0), row.getString(1))).filter(x => x._2.equals(content)).collect
  }

  
  def DataClean(df: DataFrame) = {
    df.rdd.filter(_ (0) != null).filter(_ (1) != null).filter(x => (x.getString(1).contains("'"))).filter(x => (x.getString(1).contains("'name'")))
      .filter(row => !row.getString(1).takeRight(1).equals("'"))

  }

  def QueryOfKeywords(keywords: DataFrame, df: DataFrame, content: String) = {
    val ids = DataClean(keywords).map(row => (row.getInt(0), parse(row.getString(1).replaceAll("'", "\"").replaceAll("\\\\xa0", "")
      .replaceAll("\\\\", ""))))
      .map(x => (x._1, compact(x._2 \ "name")))
      .filter(x => x._2.contains(content)).collect.take(20)
    ids.flatMap(id => df.select("Name", "Description", "PublishYear", "RatingDistTotal").where("id==" + id._1).rdd.map {
      line => (id._1, id._2, line.getString(0), line.getString(1), line.getInt(2), line.getString(3))
    }.collect)
    
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

