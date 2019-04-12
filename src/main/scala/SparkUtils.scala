import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkUtils {

  def getContext(app: String, args: Array[String]): SparkContext = {
    // scala版本用的2.12，之前运行不出错，后来运行报错：java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
    // scala版本改为2.11，运行正常

    val conf = new SparkConf().setAppName(app)

    // setMaster("local") 本机的spark就用local，远端的就写ip
    // 如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    // local代表一个线程（Executor、核），local[N]代表N个线程，local[*]代表同CPU核数量
    // 测试local[100]，也能跑起来
    conf.setMaster("local[*]")

    // 运行时4C1G

    val sc = new SparkContext(conf)

    // 简单写法
    // val sc = new SparkContext("local[*]", app)

    sc
  }

  def getSession(app: String, args: Array[String]): SparkSession = {
    // Sacla版本2.12下，报错：java.lang.NoSuchMethodError: scala.Predef$.refArrayOps([Ljava/lang/Object;)Lscala/collection/mutable/ArrayOps;
    // Scala版本改为2.11，运行正常

    // Spark自带的Example中，SparkSession方式较为常用；SparkDemo采用的SparkContext方式不常用
    val ss = SparkSession.builder().appName(app).master("local[*]")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    // .config("spark.driver.host", "localhost")解决以下报错
    // Exception in thread "main" java.net.BindException: Can't assign requested address: Service 'sparkDriver' failed after 16 retries (on a random free port)! Consider explicitly setting the appropriate binding address for the service 'sparkDriver' (for example spark.driver.bindAddress for SparkDriver) to the correct binding address.

    //.config("spark.default.parallelism", "4")

    //println(ss.sparkContext.defaultParallelism)
    //println(ss.sparkContext.defaultMinPartitions)
    //ss.sparkContext.getConf.getAll.foreach(println(_))

    ss
  }

  def getStreamContext(app: String, args: Array[String], seconds: Long): StreamingContext = {
    val ssc = new StreamingContext("local[*]", app, Seconds(seconds))

    ssc
  }

  def printRdd[T](method: String, rdd: RDD[T]): Unit = {
    val rdd2 = rdd.collect()
    print(method + ": ")
    rdd2.foreach(x => print(x + " "))
    println()
  }

  def printArray[T](method: String, array: Array[T]): Unit = {
    print(method + ": ")
    array.foreach(x => print(x + " "))
    println()
  }

}
