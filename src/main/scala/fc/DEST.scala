package fc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level


object DEST {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    
    val spark = SparkSession
        .builder()
        .appName("Follower Count")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "logs")
        .getOrCreate()
    
    if (args.length != 2) {
      logger.error("Usage:\nfc.DEST <input dir> <output dir>")
      System.exit(1)
    }

		// Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    import spark.implicits._
    val sqlContext = spark.sqlContext
    
    val lines = sqlContext.read.text(args(0)).as[String]
    val users = lines.map(_.split(",")(1))
       
    val counts = users
                 .groupBy("value")
                 .count()
    
    print(counts.explain(extended = true))
    counts.rdd.map(_.toString())saveAsTextFile(args(1))

  }
}