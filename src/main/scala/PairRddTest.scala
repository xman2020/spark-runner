
object PairRddTest {

  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getContext("PairRddTest", args)

    val rdd = sc.parallelize(Array((1, 2), (3, 4), (3, 6), (3, 8)), 2)

    SparkUtils.printRdd("rdd", rdd)

    // 按键Reduce求和，rdd.reduceByKey(_ + _): (1,2) (3,18)
    SparkUtils.printRdd("rdd.reduceByKey(_ + _)", rdd.reduceByKey(_ + _))

    // 按键分组，rdd.groupByKey(): (1,CompactBuffer(2)) (3,CompactBuffer(4, 6, 8))
    SparkUtils.printRdd("rdd.groupByKey()", rdd.groupByKey())

    // combineByKey有点小复杂
    // 分区为1，rdd.combineByKey_1: (1,2_) (3,4_@6@8)
    // 分区为2，rdd.combineByKey_1: (1,2_) (3,4_$6_@8)
    // 分区为3，rdd.combineByKey_1: (3,4_$6_@8) (1,2_)
    // 分区为4，rdd.combineByKey_1: (1,2_) (3,4_$6_$8_)
    // 分区为5，rdd.combineByKey_1: (1,2_) (3,4_$6_$8_) 感觉稳定了
    SparkUtils.printRdd("rdd.combineByKey_1", rdd.combineByKey(
      (v: Int) => v + "_",
      (c: String, v: Int) => c + "@" + v,
      (c1: String, c2: String) => c1 + "$" + c2
    ))

    // 算学生考试平均分的例子
    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    val d2 = d1.combineByKey(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map { case (name, (num, socre)) => (name, socre / num) }
    // rdd.combineByKey_2: (Wilma,95.33333333333333) (Fred,91.33333333333333)
    SparkUtils.printRdd("rdd.combineByKey_2", d2)

    // 值的每个元素执行，rdd.mapValues(_ + 1): (1,3) (3,5) (3,7) (3,9)
    SparkUtils.printRdd("rdd.mapValues(_ + 1)", rdd.mapValues(_ + 1))

    // 值的每个元素执行后扁平化，rdd.flatMapValues(_.to(5)): (1,2) (1,3) (1,4) (1,5) (3,4) (3,5)
    SparkUtils.printRdd("rdd.flatMapValues(_.to(5))", rdd.flatMapValues(_.to(5)))

    // 取键的元素，rdd.keys: 1 3 3 3
    SparkUtils.printRdd("rdd.keys", rdd.keys)

    // 取值的元素，rdd.values: 2 4 6 8
    SparkUtils.printRdd("rdd.values", rdd.values)

    // 键排序，rdd.sortByKey(fasle): (3,4) (3,6) (3,8) (1,2)
    // 默认从小到大，false从大到小
    SparkUtils.printRdd("rdd.sortByKey(fasle)", rdd.sortByKey(false))

    val rdd2 = sc.parallelize(Array((2, 7), (3, 9)))

    SparkUtils.printRdd("rdd", rdd)
    SparkUtils.printRdd("rdd2", rdd2)

    // 去除相同键的元素，rdd.subtractByKey(rdd2): (1,2)
    SparkUtils.printRdd("rdd.subtractByKey(rdd2)", rdd.subtractByKey(rdd2))

    // 内连接，rdd.join(rdd2): (3,(4,9)) (3,(6,9)) (3,(8,9))
    SparkUtils.printRdd("rdd.join(rdd2)", rdd.join(rdd2))

    // 左外连接，rdd.leftOuterJoin(rdd2): (1,(2,None)) (3,(4,Some(9))) (3,(6,Some(9))) (3,(8,Some(9)))
    SparkUtils.printRdd("rdd.leftOuterJoin(rdd2)", rdd.leftOuterJoin(rdd2))

    // 右外连接，rdd.rightOuterJoin(rdd2): (2,(None,7)) (3,(Some(4),9)) (3,(Some(6),9)) (3,(Some(8),9))
    SparkUtils.printRdd("rdd.rightOuterJoin(rdd2)", rdd.rightOuterJoin(rdd2))

    // 相同键的值组合在一起，rdd.cogroup(rdd2): (1,(CompactBuffer(2),CompactBuffer())) (2,(CompactBuffer(),CompactBuffer(7))) (3,(CompactBuffer(4, 6, 8),CompactBuffer(9)))
    SparkUtils.printRdd("rdd.cogroup(rdd2)", rdd.cogroup(rdd2))

    // 条件过滤，rdd.filter(_._2 < 5): (1,2) (3,4)
    // 另一种写法，rdd.filter { case (x, y) => y < 5 }
    SparkUtils.printRdd("rdd.filter(_._2 < 5)", rdd.filter(_._2 < 5))

    // 统计同样键的元素个数，rdd.countByKey(): Map(1 -> 1, 3 -> 3)
    println("rdd.countByKey(): " + rdd.countByKey())

    // 同样的键，值新的会把旧的覆盖，rdd.collectAsMap(): Map(1 -> 2, 3 -> 8)
    println("rdd.collectAsMap(): " + rdd.collectAsMap())

    // 查找键的值，rdd.lookup(3): WrappedArray(4, 6, 8)
    println("rdd.lookup(3): " + rdd.lookup(3))

    sc.stop()
  }

}
