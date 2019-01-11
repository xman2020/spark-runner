
object SparkDemo {

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getContext("SparkDemo", args)

    // 将集合每个元素乘以3
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6)).map(_ * 3)

    // 过滤大于10的元素
    val mappedRDD = rdd.filter(_ > 10).collect()

    // 对集合求和
    println(rdd.reduce(_ + _))

    // 输出大于10的元素
    for (arg <- mappedRDD)
      print(arg + " ")

    println()

    println("SparkDemo is finished!")

    sc.stop()

    // 如果用spark-submit提交运行
    // spark-submit --class SparkDemo --master spark://ip:7077 spark-runner.jar
  }

}
