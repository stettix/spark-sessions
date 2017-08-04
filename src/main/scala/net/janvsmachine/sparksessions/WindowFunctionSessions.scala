package net.janvsmachine.sparksessions

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._


case class Click(userId: String, targetId: String, timestamp: Long)

case class Session(userId: String, startTime: Long, endTime: Long, count: Long)

/**
  * Common code for sessions implementations
  */
trait Sessions extends LazyLogging {

  self: Spark =>

  def main(args: Array[String]): Unit = {
    if (args.length < 3 || args.length > 3) {
      println(s"Usage: ${getClass.getSimpleName.dropRight(1)} <input path> <output path> [local]")
      System.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val localSpark = args.length == 3 && args(2) == "local"

    implicit val spark: SparkSession = createSession(localSpark)

    val clicks = loadClicks(inputPath)
    val sessionized = sessionize(clicks, maxSessionDuration = 30000)

    sessionized.write.mode(SaveMode.Overwrite).csv(outputPath)
  }

  def loadClicks(path: String)(implicit spark: SparkSession): Dataset[Click] = {
    import spark.implicits._
    spark.read.option("header", "true").csv(path)
      .map { case Row(userId: String, documentId: String, timestamp: String, _, _, _) => Click(userId, documentId, timestamp.toLong) }
  }

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session]

}
