import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSQL {

  def main(args: Array[String]) {

    val sc = SparkContext;
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.json("D:\\Spark\\generated.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
//    df.select($"name", $"age" + 1).show()
//    df.filter(df("age") > 21).show()
//    df.groupBy("age").count().show()
//    df.select(avg("age")).show()
      }
}
