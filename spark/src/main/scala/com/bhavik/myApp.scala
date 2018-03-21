package com.bhavik

/**
  * Created by bhavik on 11/03/18.
  */

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext

object myApp {
  def main(args: Array[String]): Unit = {

    // creating spark conf and context
    val conf = new SparkConf().setAppName("daily_revenue_app")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // repartition to set 2
    sqlContext.setConf("spark.sql.shuffle.partitions", "2")

    //read parquet data
    val orderDF = sqlContext.read.parquet(args(0))
    val orderItemsDF = sqlContext.read.parquet(args(1))

    // register df to temp table
    orderDF.registerTempTable("orders")
    orderItemsDF.registerTempTable("order_items")

    val dailyRevenue = sqlContext.sql(
      """
        |SELECT from_unixtime(o.order_date/1000) as order_date,sum(oi.order_item_subtotal) as daily_revenue
        |FROM orders o join order_items oi on o.order_id = oi.order_item_order_id
        |WHERE o.order_status IN('COMPLETE')
        |GROUP BY from_unixtime(o.order_date/1000)
        |ORDER BY daily_revenue DESC
      """.stripMargin)

    // save result to hdfs as parquet file
    dailyRevenue.write.mode("overwrite").parquet(args(2))
  }
}
