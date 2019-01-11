import org.apache.spark._
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    // Sacla版本2.12下，报错：java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
    // Scala版本改为2.11，运行正常

    // Spark自带的Example中，SparkSession方式较为常用；SparkDemo采用的SparkContext方式不常用
    val spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate()

    // 读取文件行
    //val lines = spark.sparkContext.textFile("/Users/chenshuyuan/Desktop/project/bigdata/spark-runner/src/main/resources/word.txt")
    val lines = spark.read.textFile("/Users/chenshuyuan/Desktop/project/bigdata/spark-runner/src/main/resources/word.txt").rdd

    // 提取单词，一行 单词
    val words = lines.flatMap(_.split(" "))

    // 单词变map，一行 单词, 1
    val ones = words.map((_, 1));

    // 单词聚合，一行 单词，sum值
    val counts = ones.reduceByKey(_ + _)
    // 同 reduceByKey((a, b) => a + b)

    // 多种迭代用法
    // counts.foreach(println)

    // counts.foreach(x => println(x._1 + ": " + x._2))

    //    counts.foreach(x => {
    //      println(x._1 + ": " + x._2)
    //    })

    for (x <- counts) {
      println(x._1 + ": " + x._2)
    }

    //    hadoop: 3
    //    a: 2
    //    popular: 1
    //    platform: 1
    //    good: 1
    //    is: 3
    //    : 1
    //    hello: 2
    //    bigdata: 2
    //    world: 1
    //    technic: 1

    println("WordCount is finished!")

    spark.stop()
  }

}
