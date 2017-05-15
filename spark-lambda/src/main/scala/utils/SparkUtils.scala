package utils

import java.lang.management.ManagementFactory

import config.Settings
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ragu on 5/13/2017.
  */
object SparkUtils {

  // WebLog config
  val config = Settings.WebLogGen
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName : String ) = {

    var checkPointDirectory = ""
    val conf = new SparkConf().setAppName(appName)

    if (isIDE) {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      conf.setMaster("local[*]")
      checkPointDirectory = config.localCheckpointDir
    } else {
      checkPointDirectory = config.hdfsCheckpointDir
    }

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkPointDirectory)
    sc
  }

  def getSQLContext (sc : SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }

}
