package com.edu.neu.csye7200.finalproject

import com.edu.neu.csye7200.finalproject.collections.DataUtil
import org.apache.spark.mllib.recommendation.Rating
import org.scalatest.{FlatSpec, Matchers}


class TestData extends FlatSpec with Matchers {

  behavior of "getAllRating"
  it should "Read the header of the file and construct RDD" in {
    val test = getClass.getResource("/test_data.csv").getPath
    DataUtil.getAllRating(test).collect should matchPattern {
      case Array((9, Rating(1, 110, 1.0)), (5, Rating(1, 147, 4.5)), (9, Rating(2, 5, 3.0)))  =>
    }
  }


  behavior of "getRatingByUser"
  it should "Get rating record by userID which is set to 1" in {
    val test = getClass.getResource("/test_data.csv").getPath
    DataUtil.getRatingByUser(test,1).collect should matchPattern {
      case Array(Rating(1, 110, 1.0), Rating(1, 147, 4.5))  =>
    }
  }


}
