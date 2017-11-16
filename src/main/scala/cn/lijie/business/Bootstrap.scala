package cn.lijie.business

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/26 
  * Time: 11:38  
  */
object Bootstrap {

  /**
    * 二分查找
    *
    * @param arr
    * @param ip
    * @return
    */
  def binarySearch(arr: Array[(String, String, String, String)], ip: Long): Int = {
    var l = 0
    var h = arr.length - 1
    while (l <= h) {
      var m = (l + h) / 2
      if ((ip >= arr(m)._1.toLong) && (ip <= arr(m)._2.toLong)) {
        return m
      } else if (ip < arr(m)._1.toLong) {
        h = m - 1
      } else {
        l = m + 1
      }
    }
    -1
  }

  /**
    * IP转long
    *
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val arr = ip.split("[.]")
    var num = 0L
    for (i <- 0 until arr.length) {
      num = arr(i).toLong | num << 8L
    }
    num
  }

  def main(args: Array[String]): Unit = {


    //    print(3395782400.00.toLong)
    //1,3708713472.00,3708715007.00,"河南省","信阳市","联通","221.14.122.0","221.14.127.255"
    //id  下界  上界  省份  城市  运营商  ip段下界   ip段下界
    //这里对IP.txt里面的内容进行排序,安装上界的升序排
    val conf = new SparkConf().setMaster("local[2]").setAppName("ip")
    val sc = new SparkContext(conf)
    //TODO
    sc.textFile("src/main/file/*.txt").persist(StorageLevel.MEMORY_ONLY)
    val rdd1 = sc.textFile("src/main/file/*.txt").map(x => {
      val s = x.split(",")
      //下界  上界  省份  运营商
      (s(1), s(2), s(3), s(5))
    }).sortBy(_._1)

    //广播变量
    val bd = sc.broadcast(rdd1.collect)

    val rdd2 = sc.textFile("src/main/file/*.info").map(x => {
      val s = x.split(",")
      //(ip,1)
      (s(1), 1)
    }).reduceByKey(_ + _).sortBy(_._2)

    rdd2.map(x => {
      val ipLong = ip2Long(x._1)
      //获取下标
      val index = binarySearch(bd.value, ipLong)
      //没找到的返回unknown
      if (index == -1) {
        (ipLong, x._1, x._2, "unknown", "unknown")
      } else {
        //获取省份
        val p = bd.value(index)._3
        //获取运营商
        val y = bd.value(index)._4
        (ipLong, x._1, x._2, p, y)
      }
    }).repartition(1).saveAsTextFile("src/main/file/out")
    sc.stop()
  }
}
