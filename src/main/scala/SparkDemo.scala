
object SparkDemo {

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getContext("SparkDemo", args)

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    SparkUtils.printRdd("rdd", rdd)

    // 将集合每个元素乘以3
    val rdd2 = rdd.map(_ * 3)
    SparkUtils.printRdd("rdd2=rdd.map(_ * 3)", rdd2)

    // 对集合求和
    println("rdd2.reduce(_ + _): " + rdd2.reduce(_ + _))

    // 过滤大于10的元素
    val rdd3 = rdd2.filter(_ > 10)
    SparkUtils.printRdd("rdd2.filter(_ > 10)", rdd3)

    println("SparkDemo is finished!")

    sc.stop()

    // 如果用spark-submit提交运行
    // spark-submit --class SparkDemo --master spark://ip:7077 spark-runner.jar
  }

}
