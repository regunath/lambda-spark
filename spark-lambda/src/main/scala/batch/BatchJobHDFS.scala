package batch

import java.lang.management.ManagementFactory

import domain._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by ragu on 4/16/2017.
  */
object BatchJobHDFS {
  def main (args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("Spark Demo")

    if(ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")){
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      conf.setMaster("local[*]")
    }else {
      println("In the else part")
      val it = ManagementFactory.getRuntimeMXBean.getInputArguments.iterator()
      while (it.hasNext){
        println(it.next())
      }
    }

    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    val sourceFile = "file:///vagrant/data.tsv"

    val input = sc.textFile(sourceFile)
    //input.foreach(println)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val inputDF = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 *60

      if(record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1),record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """ SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_hour limit 5
      """.stripMargin).cache()

    val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            FROM activity
                                            GROUP BY product, timestamp_hour """).cache().coalesce(5)


    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://localhost:9000/lambda/batch1")

    //activityByProduct.registerTempTable("activityByProduct")


    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)




  }
}
