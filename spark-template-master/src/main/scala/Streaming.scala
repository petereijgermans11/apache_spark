import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming {

  def main(args: Array[String]) {

    // CONNECT met NETSTAT: D:\Develop\netstat\netcat-1.11>nc -l -p 1234
    // TIK een paar dubbele woorden in bijv: hello world world

    val appName = "streaming"
    val master = "local[*]"
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines: DStream[String] = ssc.socketTextStream("localhost", 1234)

    val numbers: DStream[(String, Int)] = lines
      .flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    numbers.foreachRDD(_.foreach(println))

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
      }
}
