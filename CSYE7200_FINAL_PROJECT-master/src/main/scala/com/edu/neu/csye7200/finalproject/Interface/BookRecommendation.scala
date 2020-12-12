package csye7200FinalProject.Interface


import csye7200FinalProject.Collections.{ALSUtil, DataUtil, QueryUtil}
import csye7200FinalProject.Configure.FileConfig

import java.sql.Date

object BookRecommendation {

  lazy val df=DataUtil.getBooksDF

 
  def getRecommendation(userId: Int) = {
    //RDD[long, Rating]
    val ratings = DataUtil.getAllRating(FileConfig.ratingFile)

    val booksArray = DataUtil.getBooksArray

    val links = DataUtil.getLinkData(FileConfig.linkFile)
    val books = DataUtil.getCandidatesAndLink(booksArray, links)

    val userRatingRDD = DataUtil.getRatingByUser(FileConfig.ratingFile, userId)
    val userRatingBook = userRatingRDD.map(x => x.product).collect
    val numRatings = ratings.count()
    val numUser = ratings.map(_._2.user).distinct().count()
    val numBook = ratings.map(_._2.product).distinct().count()

    println("rating: " + numRatings + " books: " + numBook
      + " user: " + numUser)

    // Split data into train(60%), validation(20%) and test(20%)
    val numPartitions = 10

    val trainSet = ratings.filter(x => x._1 < 6).map(_._2).
      union(userRatingRDD).repartition(numPartitions).persist()
    val validationSet = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .map(_._2).persist()
    val testSet = ratings.filter(x => x._1 >= 8).map(_._2).persist()

    val numTrain = trainSet.count()
    val numValidation = validationSet.count()
    val numTest = testSet.count()

    println("Training data: " + numTrain + " Validation data: " + numValidation
      + " Test data: " + numTest)

    //      Train model and optimize model with validation set
    ALSUtil.trainAndRecommendation(trainSet, validationSet, testSet, books, userRatingRDD)
  }
  
  def queryBySelectedInBookJson(content: String, SelectedType: String) = {
    QueryUtil.QueryBookJson(df, content, SelectedType)
  }

  def queryBySeletedInBooksNormal(content: String, SelectedType: String) = {
    QueryUtil.QueryBookInfoNorm(df, content, SelectedType)
  }

  def queryByKeywords(content: String) = {
    //Query of keywords
    val keywordsRDD = DataUtil.getKeywords(FileConfig.keywordsFile)
    QueryUtil.QueryOfKeywords(keywordsRDD, df, content)
  }
 
  def SortBySelected(ds:Array[(Int,String,String,String,Date,Double)],selectedType:String="popularity",order:String="desc")= {
    selectedType match {
      case "popularity" => order match {
        case "desc" => ds.sortBy(-_._6)
        case "asc" => ds.sortBy(_._6)
      }
      case "release_date" =>
        order match {
          case "desc" => ds.sortWith(_._5.getTime > _._5.getTime)
          case _ => ds.sortBy(-_._5.getTime)
        }
    }
  }


  def FindBookByName(BookName: String) = {
    val id = QueryUtil.QueryBookIdByName(df, BookName).map(x => x._1)
    if (id.length != 0)
      Some(id)
    else None
  }


  def insert(list: List[String], i: Int, value: Int) = {
    list.take(i) ++ List(value.toString) ++ list.drop(i)
  }
}
