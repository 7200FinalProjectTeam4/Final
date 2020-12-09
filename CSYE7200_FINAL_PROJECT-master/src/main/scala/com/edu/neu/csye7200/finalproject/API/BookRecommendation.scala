package com.edu.neu.csye7200.finalproject.API

import com.edu.neu.csye7200.finalproject.collections.{ALSUtil, DataUtil}
import com.github.tototoshi.csv._

object BookRecommendation {
  lazy val df = DataUtil.getBooksDF

  /**
    * This function trained the data and get the recommendation book
    * for specific user and print the result.
    *
    * @param userId The specific user of recommendation
    * @return Return the RMSE and improvement of the Model
    */
  def getRecommendation(userId: Int) = {
    //RDD[long, Rating]
    val time1 = System.currentTimeMillis()

    val ratings = DataUtil.getAllRating("input/BX-Book-Ratings.csv")

    val books = DataUtil.getBooksArray

    val userRatingRDD = DataUtil.getRatingByUser("input/BX-Book-Ratings.csv", userId)
    val time2 = System.currentTimeMillis()
    val time = time2 - time1
    println("-")


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
    println("Computing different ALS models.. ")
    println("-")
    val time3= System.currentTimeMillis()
    //      Train model and optimize model with validation set
    val x= ALSUtil.trainAndRecommendation(trainSet, validationSet, testSet, books, userRatingRDD)
    val time4= System.currentTimeMillis()
    val newtime=time4-time3
    println("-")
    println("Time consumed for processing data: [" + time + " ms]")
    println("Time consumed for training and testing each model: [" + newtime/8 + " ms]")
    x
  }

  /**
    * Search Books by book name
    *
    * @param BookName The user input of book name
    * @return Option of Array of [Int]
    */
  def FindBookByName(BookName: String) = {
    val id = DataUtil.QueryBookIdByName(df, BookName).map(x => x._1)
    if (id.length != 0)
      Some(id)
    else None
  }

  def GetBookByName(BookName: String) = {
    val id = DataUtil.QueryByName(df, BookName)
    if (id.length != 0)
      Some(id)
    else None
  }

  def GetBookByPublishYear(BookName: String) = {
    val books = DataUtil.QuerybyPublishyear(df, BookName)
    if (books.length != 0)
      Some(books)
    else None
  }


  def insert(list: List[String], i: Int, value: Int) = {
    list.take(i) ++ List(value.toString) ++ list.drop(i)
  }

  def UpdateRatingsByRecommendation(RatingsInfo: List[String], BookName: String) = {
    val time7=System.currentTimeMillis()
    val bookId = FindBookByName(BookName).getOrElse(Array())
    val writer = CSVWriter.open("input/BX-Book-Ratings.csv", append = true)
    if (bookId.nonEmpty) {
      val links = DataUtil.getLinkData("input/Book-Links.csv")
      val imdbId = DataUtil.bookIdTransfer(bookId, links)
      writer.writeRow(insert(RatingsInfo, 1, imdbId(0)))
      println("Book Rating is Updated Successfully")
      val time8=System.currentTimeMillis()
      val newtime2= time8-time7
      println("Time Consumed for updating rating:["+newtime2+" ms]")
    }
    else println("Cannot find the book")
    writer.close()

  }


}


