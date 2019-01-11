import org.apache.spark._

object SparkDemo {

  def main(args: Array[String]): Unit = {
    // scala版本用的2.12，之前运行不出错，后来运行报错：java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
    // scala版本改为2.11，运行正常

    val conf = new SparkConf().setAppName("SparkDemo")
    // setMaster("local") 本机的spark就用local，远端的就写ip
    // 如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    // local代表一个线程（Executor、核），local[N]代表N个线程，local[*]代表同CPU核数量
    // 测试local[100]，也能跑起来
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

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
