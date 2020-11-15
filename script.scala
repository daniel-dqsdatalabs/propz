
//==============================================================================
// filename          : script.scala 
// description       : computes amount of purchases for middle-aged customers by each day of month from an aleatory date
// author            : daniel
// email             : daniel@dqsdatalabs.com
// date              : 14.11.2020
// version           : 0.01
//==============================================================================


import org.apache.spark.sql.SparkSession

class Customers() {
	
	var RESULTS_PATH: String = "file:" + new java.io.File(".").getCanonicalPath + "/raw_data/result"
	var CUSTOMERS_PATH: String = "file:" + new java.io.File(".").getCanonicalPath + "/raw_data/customers.csv"
	var TRANSACTIONS_PATH: String = "file:" + new java.io.File(".").getCanonicalPath + "/raw_data/transactions.csv"

	def compute_purchases_by_day(first_date: org.apache.spark.sql.Column, last_date: org.apache.spark.sql.Column) {
		var customers = spark.read.format("csv").option("header", "true").load(CUSTOMERS_PATH)
			.withColumn("customer_full_name", concat(col("name"), lit(" "), col("last_name"))).filter(!(col("age") < 30 || col("age") > 50))

		var transactions = spark.read.format("csv").option("header", "true").load(TRANSACTIONS_PATH)
			.withColumn("day", dayofmonth(col("date"))).filter(col("date").between(first_date, last_date))

		var transactionsByCustomers = transactions.join(broadcast(customers), transactions.col("customer") === customers.col("customer"))

		transactionsByCustomers
			.repartition(col("day"), col("customer_full_name"))
			.groupBy(col("customer_full_name"), col("day"))
			.agg(sum(col("amount")).alias("amount"))
			.write
			.format("parquet")
			.mode("overwrite")
			.save(RESULTS_PATH)
	}

	def print_results() {
		spark.read.format("parquet").load(RESULTS_PATH).orderBy(col("day").asc).show(1000)
	}
}

object Propz {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("propz")
        .master("local[4]")
        .getOrCreate()

	import spark.implicits._
	import spark.sqlContext.implicits._
	import org.apache.spark.sql.functions._

	val start_time = System.nanoTime()

	val curr_date = "2020-06-18"  
	val last_date = last_day(lit(curr_date))
	val first_date = trunc(lit(curr_date), "month")
	
	val customers = new Customers();
	customers.compute_purchases_by_day(first_date, last_date);
	customers.print_results();
	
	val end_time = System.nanoTime()
	println("Elapsed time: " + (end_time - start_time)/10e8 + "s")

  }
}


