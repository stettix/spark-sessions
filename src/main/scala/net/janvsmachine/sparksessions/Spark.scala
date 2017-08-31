package net.janvsmachine.sparksessions

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.spark.sql.SparkSession

trait Spark {

  def createSession(local: Boolean): SparkSession = {
    val builder = SparkSession.builder().appName(this.getClass.getSimpleName)

    if (local) {
      val session = builder.master("local[*]").getOrCreate()
      // Get credentials so that jobs run locally (e.g. for debugging) can access AWS.
      val credentials = new DefaultAWSCredentialsProviderChain().getCredentials
      session.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", credentials.getAWSAccessKeyId)
      session.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", credentials.getAWSSecretKey)
      session
    } else builder.getOrCreate()

  }
}
