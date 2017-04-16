package batch

import java.lang.management.ManagementFactory

import domain._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by ragu on 4/16/2017.
  */
object BatchJobSQL {
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

    val sourceFile = "file:///d:/work-shop/spark-workspace/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"

    val input = sc.textFile(sourceFile)
    //input.foreach(println)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    sqlContext.udf.register("UnderExposed", (pageViweCount: Long, purchaseCount: Long)  => if(purchaseCount == 0) 0 else pageViweCount / purchaseCount);

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
    df.printSchema()

    val visitorsByProduct = sqlContext.sql(
      """ SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_hour limit 5
      """.stripMargin)

    visitorsByProduct.printSchema()
    val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            FROM activity
                                            GROUP BY product, timestamp_hour """)

    activityByProduct.registerTempTable("activityByProduct")
    activityByProduct.printSchema()
    //activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

//    Not sure the column product is not found in this dataframe, need to check on the same.
//        val underExposedProducts = sqlContext.sql(
//          """SELECT
//            product,
//            timestamp_hour,
//            UnderExposed(page_view_count, purchase_count) as negative_exposure,
//            FROM activityByProduct
//            ORDER BY negative_exposure desc
//            LIMIT 5
//          """.stripMargin).cache()
//
//        underExposedProducts.printSchema()

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
    //underExposedProducts.foreach(println)




  }
}
