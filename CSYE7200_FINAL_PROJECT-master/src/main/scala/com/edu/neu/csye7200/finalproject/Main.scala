package com.edu.neu.csye7200.finalproject
import com.edu.neu.csye7200.finalproject.Interface.BookRecommendation
import com.edu.neu.csye7200.finalproject.configure.FileConfig
import com.edu.neu.csye7200.finalproject.util.DataUtil
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

object Main extends App {
  def ToInt(line: String): Try[Int] = {
    Try(line.toInt)
  }

  override def main(args: Array[String]): Unit = {
    breakable {
      while (true) {
        println("\n|-----------------------------------------------|" +
          "\n|--------------------Welcome--------------------|" +
          "\n|----------Book Recommendation System-----------|" +
          "\n|-----------------------------------------------|" +
          "\n|-----------------------------------------------|")
        println("\nPlease Enter Your ID to Sign Into Our Cool System: ")
        val id = scala.io.StdIn.readLine()
        if (id.equals("q")) break
        breakable {
          while (true) {
            ToInt(id) match {
              case Success(t) => {
                println("\n|-----------------------------------------------|" +
                  "\n|----------Book Recommendation System-----------|" +
                  "\n|-----------------------------------------------|" +
                  "\n|------------------User:"+ id + "---------------------|" +
                  "\n|-----------------------------------------------|"
                )
                println("")
                println("1.Best Books by Year" +
                  "\n2.Search Books" +
                  "\n3.Rate Books" +
                  "\n4.Recommend Books"+
                  "\nq.Quit (Return to Previous Menu)")
                var  num = scala.io.StdIn.readLine()
                if(num.equals("q")) break
                breakable {
                  while (true) {
                    ToInt(num) match {
                      case Success(t)=>{
                        t match {
                          case 1 => {
                            println("|-----------------------------------------------|" +
                              "\n|---------------Best Books by Year--------------|" +
                              "\n|-----------------------------------------------|" +
                              "\n|--------------------User:" + id + "-------------------|" +
                              "\n|-----------------------------------------------|")
                            println("")
                            println("Please Select The Year: ")
                            num = scala.io.StdIn.readLine()
                            if (num.equals("q")) break
                            num.toInt match {
                              case num => {
                                DataUtil.get2020BestBook(num)
                                break
                              }
                              case _ => {
                                println("Cannot Find The Year"); break
                              }
                            }
                          }
                          case 2 => {
                              breakable {
                                while (true) {
                                  println("|----------------------------------------------|" +
                                        "\n|-----------------Search Books-----------------|" +
                                        "\n|----------------------------------------------|" +
                                        "\n|------------------User:" + id + "--------------------|" +
                                        "\n|----------------------------------------------|")
                                  println("1.Search by Name" +
                                    "\n2.Search by ISBN" +
                                    "\n3.Search by Author" +
                                    "\n4.Search by Publisher" +
                                    "\nq.Quit (Return to Previous Menu)"
                                  )
                                  println("")
                                  println("Please Select The Way to Search:")
                                  num = scala.io.StdIn.readLine()
                                  if (num.equals("q")) break
                                  ToInt(num) match {

                                    case Success(v) => {
                                      println("Please Enter Content:")
                                      val content = scala.io.StdIn.readLine()
                                      println("")
                                      v match {
                                        case 1 => BookRecommendation.searchByName(content).take(10)
                                          .foreach(line => println("Id: " + line._1, " Name: " + line._2, " Authors: " + line._3, " Publisher: " + line._4, " PublishYear: " + line._5, " ISBN: " + line._6, " Rating: " + line._7));
                                          println("")
                                        case 2 => BookeRecommendation.searchByISBN(content)
                                          .foreach(line => println("Id: " + line._1, " Name: " + line._2, " Authors: " + line._3, " Publisher: " + line._4, " PublishYear: " + line._5, " ISBN: " + line._6, " Rating: " + line._7));
                                          println("")
                                        case 3 => BookRecommendation.searchByAuthor(content).take(10)
                                          .foreach(line => println("Id: " + line._1, " Name: " + line._2, " Authors: " + line._3, " Publisher: " + line._4, " PublishYear: " + line._5, " ISBN: " + line._6, " Rating: " + line._7));
                                          println("")
                                        case 4 => BookRecommendation.searchByPublisher(content).take(10)
                                          .foreach(line => println("Id: " + line._1, " Name: " + line._2, " Authors: " + line._3, " Publisher: " + line._4, " PublishYear: " + line._5, " ISBN: " + line._6, " Rating: " + line._7));
                                          println("")
                                        case _ => break
                                      }
                                    }
                                    case Failure(e) => break
                                  }
                                }
                              }
                          }
                          case 3 => {

                              {
                                println("|-----------------Rate Books------------------|" +
                                      "\n|---------------------------------------------|" +
                                  "\n|-------------------User:" + id + "------------------|" +
                                      "\n|---------------------------------------------|"
                                )
                                println("")
                                println("Please Enter the Book Name: ")
                                val content = scala.io.StdIn.readLine()

                                while (true) {
                                  println("Please Rate <" + content + "> (0~5):")
                                  val rating = scala.io.StdIn.readLine()

                                    if (rating.equals("q")) break
                                    Try(rating.toFloat) match {
                                      case Success(r) => {
                                        if (r >= 0 && r <= 5) {
                                          Thread.sleep(3000)
                                          println("Successfully Rated <" + content + "> for " + rating + "! Thank you!")
                                          println(""); break
                                        }
                                        else {
                                          println("out of range")
                                        }
                                      }
                                      case Failure(r) =>
                                    }

                                }
                              }


                          }
                          case 4 => {
                            println("Please Wait...")
                            BookRecommendation.getRecommendation(id.toInt)
                            //                           scala.io.StdIn.readLine()
                            break
                          }
                            case _ => break
                          }
                        }
                      case Failure(e)=>break
                    }
                  }
                }
              }
              case Failure(e)=> break
            }

          }
        }
      }
    }
  }
}
