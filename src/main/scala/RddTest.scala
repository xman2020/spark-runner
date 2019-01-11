import org.apache.spark.rdd.RDD

object RddTest {

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getContext("RddTest", args)

    val rdd = sc.parallelize(List(1, 2, 3, 3))

    this.printRdd("rdd", rdd)

    // 每个元素执行，rdd.map(_ + 1): 2 3 4 4
    this.printRdd("rdd.map(_ + 1)", rdd.map(_ + 1))

    // 每个元素执行后扁平化，rdd.flatMap(_.to(3)): 1 2 3 2 3 3 3
    this.printRdd("rdd.flatMap(_.to(3))", rdd.flatMap(_.to(3)))

    // 条件过滤，rdd.filter(_ < 3): 1 2
    this.printRdd("rdd.filter(_ < 3)", rdd.filter(_ < 3))

    // 去重，rdd.distinct(): 1 2 3
    this.printRdd("rdd.distinct()", rdd.distinct())

    // 采样，不确定
    this.printRdd("rdd.sample(false, 0.5)", rdd.sample(false, 0.5))

    val rdd2 = sc.parallelize(List(3, 7, 8))

    this.printRdd("rdd2", rdd2)

    // 联合，rdd.union(rdd2): 1 2 3 3 3 7 8
    this.printRdd("rdd.union(rdd2)", rdd.union(rdd2))

    // 取相同元素，rdd.intersection(rdd2): 3
    this.printRdd("rdd.intersection(rdd2)", rdd.intersection(rdd2))

    // 去除相同元素，rdd.subtract(rdd2): 1 2
    this.printRdd("rdd.subtract(rdd2)", rdd.subtract(rdd2))

    // 笛卡尔积，rdd.cartesian(rdd2): (1,3) (1,7) (1,8) (2,3) (2,7) (2,8) (3,3) (3,7) (3,8) (3,3) (3,7) (3,8)
    this.printRdd("rdd.cartesian(rdd2)", rdd.cartesian(rdd2))

    val rdd3 = sc.parallelize(List(3, 10, 5, 7, 5))

    this.printRdd("rdd3", rdd3)

    // 统计元素个数，rdd3.count(): 5
    println("rdd3.count(): " + rdd3.count())

    // 统计同样元素的个数，rdd3.countByValue(): Map(5 -> 2, 10 -> 1, 3 -> 1, 7 -> 1)
    println("rdd3.countByValue(): " + rdd3.countByValue())

    // 从前取N个元素，rdd3.take(2): 3 10
    this.printArray("rdd3.take(2)", rdd3.take(2))

    // 默认从大到小排序后取前N个元素，rdd3.top(3) descend: 10 7 5
    this.printArray("rdd3.top(3) descend", rdd3.top(3))

    // 从小到大排序后取前N个元素，rdd3.top(3) ascend: 3 5 5
    this.printArray("rdd3.top(3) ascend", rdd3.top(3)(implicitly[Ordering[Int]].reverse))

    // 随机取N个元素
    this.printArray("rdd3.takeSample(false, 2)", rdd3.takeSample(false, 2))

    // Reduce求和，rdd3.reduce(_ + _): 30
    println("rdd3.reduce(_ + _): " + rdd3.reduce(_ + _))

    // Reduce求和，每个分区加N，rdd3.fold(1)(_ + _): 35
    println("rdd3.fold(1)(_ + _): " + rdd3.fold(2)(_ + _))

    // aggregate有点小复杂，rdd3.aggregate(...): (30,5)
    println("rdd3.aggregate(...): " + rdd3.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2)))

    sc.stop()
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
