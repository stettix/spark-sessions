package net.janvsmachine.sparksessions

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._


case class Click(userId: String, targetId: String, timestamp: Long)

case class Session(userId: String, startTime: Long, endTime: Long, count: Long)

/**
  * Common code for sessions implementations
  */
trait Sessions extends LazyLogging {

  self: Spark =>

  import Sessions._

  def main(args: Array[String]): Unit = {
    if (args.length < 2 || args.length > 3) {
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

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session]

}

object Sessions {

  def loadClicks(path: String)(implicit spark: SparkSession): Dataset[Click] = {
    import spark.implicits._
    spark.read.parquet(path)
      .map(row => Click(row.getAs[String]("uuid"), row.getAs[Int]("document_id").toString, row.getAs[Int]("timestamp").toLong + 1465876799998L))
  }

}
