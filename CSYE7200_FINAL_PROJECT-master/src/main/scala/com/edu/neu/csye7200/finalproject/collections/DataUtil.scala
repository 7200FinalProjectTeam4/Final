package csye7200FinalProject.Collections


import csye7200FinalProject.Schema.BookSchema
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
object DataUtil {

  lazy val spark = SparkSession
    .builder()
    .appName("BookRecommondation")
    .master("local[*]")
    .getOrCreate()
  lazy val bookDF=spark.read.option("header", true).schema(BookSchema. bookSchema).csv("Book1_100k.csv")

  /**
   * Get RDD object from ratings.csv file
   * @param file   The path of the file
   * @return       RDD of [[(Long, Rating)]] with( user, product, rating)
   */
  def getAllRating(file: String) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      // (timestamp, user, product, rating)
      (fields(3).toLong%10,
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat))
    }
  }

  def QueryBookIdByName(df:DataFrame,content:String)={
    val colsList= List(col("Id"), col("Name"))
    df.select(colsList: _*).rdd.filter(_(0)!= null).filter(_(1)!=null).map(row => (row.getInt(0), row.getString(1))).filter(x=>x._2.equals(content)).collect
  }

  /**
   * Get all the movie data of Array type
   * @return       Array of [[(Int, String)]] contiaining (movieId, title)
   */
  def getBooksArray  = {
    import spark.implicits._
    // There are some null id in movies data and filter them out
    bookDF.select($"Id", $"Name").collect().filter(_(0) != null).map(x => (x.getInt(0), x.getString(1)))
  }


  def getBooksDF = bookDF

  /**
   * Get the rating information of specific user
   * @param file   The path of the file
   * @param userId user Id
   * @return       RDD of[[Rating]] with (user, product, rating)
   */
  def getRatingByUser(file: String, userId: Int) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      // (timestamp, user, product, rating)
      (fields(3).toLong, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }.filter(row => userId == row._2.user)
      .map(_._2)
  }

  /**
   * Get the id
   * @param file   The path of file
   * @return       Map of [[Int, Int]]
   */
  def getLinkData(file: String) = {
    val df = spark.read.option("header", true).schema(BookSchema.linkdataSchema).csv(file)
    import spark.implicits._
    df.select($"bookId", $"greeksId").collect.filter(_(1) != null).map(x => (x.getInt(1), x.getInt(0))).toMap
  }


  def bookIdTransfer(bookids: Array[Int], links: Map[Int, Int]) = {
    bookids.filter(x => links.get(x).nonEmpty).map(x => links(x))
  }


  def getCandidatesAndLink(books: Array[(Int, String)], links: Map[Int, Int]) = {
    books.filter(x => links.get(x._1).nonEmpty).map(x => (links(x._1), x._2)).toMap
  }

  /**
   * Get the keywords of movies which keywords formed in JSON format
   * @param file   The path of file
   * @return       DataFrame of keywords
   */
  def getKeywords(file: String) = {
    spark.read.option("header", true).schema(BookSchema.keywordsSchema).csv(file)
  }

}
