import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]) {
    val appName = "a"
    val master = "local"
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("d:/Spark/shakespeare.txt")
    //val rddCount = rdd.flatMap(line => line.split("\\s")).map(word => word.toLowerCase).distinct.count()

    //val rddCount = rdd.flatMap(line => line.split("\\s")).map(word => (word, 1)).reduceByKey(_ + _).max()

    val rddCount = rdd.flatMap(line => line.split("\\s")).map(word => (word, "arbitrary")).countByKey

    println("count: " + rddCount)

    sc.stop()
  }

}
