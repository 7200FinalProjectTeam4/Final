package csye7200FinalProject.Collections

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object ALSUtil {

  val numRanks = List(8, 12, 20)
  val numIters = List(10, 15, 20)
  val numLambdas = List(0.05, 0.1, 0.2)
  var bestRmse = Double.MaxValue
  var bestModel: Option[MatrixFactorizationModel] = None
  var bestRanks = -1
  var bestIters = 0
  var bestLambdas = -1.0

  /**
   * Claculate the RMSE computation
   *
   * @param model The trained model
   * @param data  RDD of [[]Rating]] objects with userID, productID, and rating
   * @return The RMSE of the model
   */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val prediction = model.predict(data.map(x => (x.user, x.product)))
    val predDataJoined = prediction.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating))).values
    new RegressionMetrics(predDataJoined).rootMeanSquaredError
  }

  /**
   * Train and optimize model with validation set
   *
   * @param trainSet      RDD of [[]Rating]] objects of training set
   * @param validationSet The validation set
   */
  def trainAndOptimizeModel(trainSet: RDD[Rating], validationSet: RDD[Rating]): Unit = {
    // Looking for the model of optimized parameter
    for (rank <- numRanks; iter <- numIters; lambda <- numLambdas) {
      val model = ALS.train(trainSet, rank, iter, lambda)
      val validationRmse = computeRmse(model, validationSet)


      if (validationRmse < bestRmse) {
        bestModel = Some(model)
        bestRmse = validationRmse
        bestIters = iter
        bestLambdas = lambda
        bestRanks = rank
      }
    }
  }

  /**
   * Evaluate model on test set
   *
   * @param trainSet      RDD of [[]Rating]] objects of training set
   * @param validationSet RDD of [[]Rating]] objects of validation set
   * @param testSet       RDD of [[]Rating]] objects of test set
   */
  def evaluateMode(trainSet: RDD[Rating], validationSet: RDD[Rating], testSet: RDD[Rating]) = {
    val testRmse = computeRmse(bestModel.get, testSet)
    println("-")
    println("The best model: With Rank-Iter-Lambda=[" + bestRanks + "," + bestIters
      + "," + bestLambdas + "] & RMSE[" + testRmse + "]")

    // Create a baseline and compare it with best model
    val meanRating = trainSet.union(validationSet).map(_.rating).mean()
    // RMSE of baseline
    val baselineRmse = new RegressionMetrics(testSet.map(x => (x.rating, meanRating)))
      .rootMeanSquaredError
    // RMSE of test (This should be smaller)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
    Array(testRmse, improvement)
  }

  /**
   * Make a personal recommendation
   *
   * @param books      Maps of movie which moviesId as key and title as value
   * @param userRating RDD of [[]Rating]] objects of specific user rating information
   * @return Array of 20 [Rating] objects
   */
  def makeRecommendation(books: Map[Int, String], userRating: RDD[Rating]) = {
    // Make a personal recommendation and filter out the book already rated.
    val bookId = userRating.map(_.product).collect.toSeq
    val candidates = DataUtil.spark.sparkContext.parallelize(books.keys.filter(!bookId.contains(_)).toSeq)
    bestModel.get
      .predict(candidates.map(x => (1, x)))
      .sortBy(-_.rating)
      .take(20)
  }

  /**
   * Combine all the function above and print out the recommendation books
   *
   * @param trainSet      RDD of [[]Rating]] objects of training set
   * @param validationSet RDD of [[]Rating]] objects of validation set
   * @param testSet       RDD of [[]Rating]] objects of test set
   * @param books         Maps of books which booksId as key and title as value
   * @param userRating    RDD of [[]Rating]] objects of specific user rating information
   */
  def trainAndRecommendation(trainSet: RDD[Rating], validationSet: RDD[Rating], testSet: RDD[Rating]
                             , books: Map[Int, String], userRating: RDD[Rating]) = {
    trainAndOptimizeModel(trainSet, validationSet)

    val recommendations = makeRecommendation(books, userRating)
    var i = 1
    println(" Recommended Book for you is :")
    recommendations.foreach { line =>
      println("%2d".format(i) + " : <<" + books(line.product) + ">>")
      i += 1
    }
    val RMSE = evaluateMode(trainSet, validationSet, testSet)
    RMSE
  }
}
