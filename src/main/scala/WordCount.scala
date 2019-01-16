
object WordCount {

  def main(args: Array[String]): Unit = {
    val ss = SparkUtils.getSession("WordCount", args)

    // 读取文件行
    //val lines = ss.sparkContext.textFile("/Users/chenshuyuan/Desktop/project/bigdata/spark-runner/src/main/resources/word.txt")
    val lines = ss.read.textFile("/Users/chenshuyuan/Desktop/project/bigdata/spark-runner/src/main/resources/word.txt").rdd

    println("文本内容:")
    lines.foreach(println)

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

    println("单词统计:")

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

    ss.stop()
  }

}
