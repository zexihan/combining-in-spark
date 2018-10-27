package fc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDD_A {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nfc.RDD_A <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Follower Count")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "logs")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))
    val counts = textFile.map(line => line.split(","))
                 .map(user => (user(1), 1))
                 .aggregateByKey(0)(_ + _, _ + _)
    
    print(counts.toDebugString)
    counts.saveAsTextFile(args(1))
  }
}