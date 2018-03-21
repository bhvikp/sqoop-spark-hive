# mysql,sqoop,spark,hive workflow pipeline
it will generate daily revenue from two tables orders and order_items

### Sqoop

  - Import orders and order_items data from mysql to hdfs in "parquet" format
  - here we are creating sqoop job for incremental data imports

##### commands 
sqoop job from orders data
```sh
# sqoop job for loda data to hdfs from mysql "retail_db" database
sqoop job --create loadorders \
-- import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password-file /project/sqoop/sqoop.password \
--table orders \
--target-dir /project/data/orders \
--as-parquetfile \
--check-column order_id \
--incremental append \
--last-value 0
```

sqoop job from order_items data
```sh
# sqoop job order_items table
sqoop job --create loadorderitems \
-- import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password-file /project/sqoop/sqoop.password \
--table order_items \
--target-dir /project/data/order_items \
--as-parquetfile \
--check-column order_item_id \
--incremental append \
--last-value 0
```

```sh
$ sqoop job --list
$ sqoop job --exec loadorders
$ sqoop job --exec loadorderitems
```


### Spark
- Spark read parquet data and create dataframe and then perform query to compute daily_revenue 
- daily revenue will be stored as parquet file in hdfs

#### code
```sh
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
```

#### execution
we need to create jar file to submit

```sh
spark-submit --master yarn \
--class com.bhavik.myApp \
--num-executors 2 \
--executor-memory 512M \
myapp_2.10-1.0.jar \
/project/data/orders /project/data/order_items /project/output/daily_revenue
```

### Hive
- here we are creating hive database and external table to read daily_revenue parquet data computed by spark

#### query
open hive shell and execute below queries
```sh
hive > CREATE DATABASE project;
hive > USE project;
hive > CREATE EXTERNAL TABLE daily_revenue(
order_date string,
daily_revenue double)
STORED AS parquet
LOCATION '/project/output/daily_revenue';
```
get data from that table
```sh
SELECT * FROM daily_revenue limit 20;
```


##### ##end
