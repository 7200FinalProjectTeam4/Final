package csye7200FinalProject.Interface


import csye7200FinalProject.Collections.{ALSUtil, DataUtil, QueryUtil}
import csye7200FinalProject.Configure.FileConfig

import java.sql.Date

object BookRecommendation {

  lazy val df=DataUtil.getBooksDF

  /**
   * This function trained the data and get the recommendation movie
   * for specific user and print the result.
   * @param userId     The specific user of recommendation
   * @return           Return the RMSE and improvement of the Model
   */
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
  /**
   * Search Json Format sInfo in movie
   *
   * @param content      The specified content
   * @param SelectedType Selected Search Type
   * @return Array of [(Int,String,String,String,Date,Double)]
   *         with (movidId,selectedType,title,tagline,release_date,popularity)
   */
  def queryBySelectedInBookJson(content: String, SelectedType: String) = {
    QueryUtil.QueryBookJson(df, content, SelectedType)
  }

  /**
   * Search String Info in movie
   *
   * @param content      The specified content
   * @param SelectedType Selected Search Type
   * @return Array of [(Int,String,String,String,Date,Double)]
   *         with (movidId,selectedType,title,tagline,release_date,popularity)
   */
  def queryBySeletedInBooksNormal(content: String, SelectedType: String) = {
    QueryUtil.QueryBookInfoNorm(df, content, SelectedType)
  }

  /**
   * Search movie by keywords
   *
   * @param content The specified keywords
   * @return Array of [(Int,String,String,String,Date,Double)]
   *         with (movidId,selectedType,title,tagline,release_date,popularity)
   */
  def queryByKeywords(content: String) = {
    //Query of keywords
    val keywordsRDD = DataUtil.getKeywords(FileConfig.keywordsFile)
    QueryUtil.QueryOfKeywords(keywordsRDD, df, content)
  }
  /**
   * Sort the Array of books
   * @param ds             The dataset of movies to be sorted
   * @param selectedType   The sort key word
   * @param order          The order type: desc or asc
   * @return               Sorted book dataset
   */
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

//  /**
//   * Search book by staffs
//   *
//   * @param content      The user input of specific staff
//   * @param SelectedType Specify the content type: crew or cast
//   * @return Array of [[Int, String, String, String, Date, Double]]
//   *         with (id, staff,title,tagline,release_date,popularity)
//   */
//  def queryBystaffInCredits(content: String, SelectedType: String) = {
//    QueryUtil.QueryOfstaff(DataUtil.getStaff(FileConfig.creditFIle), df, content, SelectedType)
//  }

  /**
   * Search books by book name
   * @param BookName    The user input of book name
   * @return             Option of Array of [Int] contains the book tmdbID
   */
  def FindBookByName(BookName: String) = {
    val id = QueryUtil.QueryBookIdByName(df, BookName).map(x => x._1)
    if (id.length != 0)
      Some(id)
    else None
  }

//  /** add rating row to csv file to have better perform train model and result
//   *
//   * @param RatingsInfo userid,movieId,rating,timestamp(System.currentTimeMillis/1000)
//   * @param MovieName   movieName
//   * @tparam T
//   */
//  def UpdateRatingsByRecommendation(RatingsInfo: List[String], MovieName: String) = {
//    val movieId = FindMovieByName(MovieName).getOrElse(Array())
//    val writer = CSVWriter.open(FileConfig.ratingFile, append = true)
//    if (movieId.nonEmpty) {
//      val links = DataUtil.getLinkData(FileConfig.linkFile)
//      val imdbId = DataUtil.bookIdTransfer(movieId, links)
//      writer.writeRow(insert(RatingsInfo, 1, imdbId(0)))
//      println("Rating Successfully")
//    }
//    else println("Cannot find the movie you entered")
//    writer.close()
//
//  }

  /** insert value into desired position
   *
   * @param list  original List
   * @param i     position desired to insert
   * @param value the insert value
   * @tparam T
   * @return desire list
   */
  def insert(list: List[String], i: Int, value: Int) = {
    list.take(i) ++ List(value.toString) ++ list.drop(i)
  }
}
