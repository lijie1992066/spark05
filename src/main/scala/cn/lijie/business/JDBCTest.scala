package cn.lijie.business

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/26 
  * Time: 16:08  
  */
object JDBCTest {

  /**
    * 持久化方法
    *
    * @param it
    */
  def persistData(it: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var pre: PreparedStatement = null
    val sql: String = "insert into jdbc_spark (w,c) VALUES (?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.80.123:3306/portrait?user=root&password=")
      it.foreach(x => {
        pre = conn.prepareStatement(sql)
        pre.setString(1, x._1)
        pre.setInt(2, x._2)
        pre.executeUpdate()
      })
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      if (pre != null) {
        pre.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  val func = (index: Int, iter: Iterator[(String, Int)]) => {
    iter.toList.map(x => {
      (index, x._1, x._2)
    }).iterator
  }

  def me(a: Int): Int = {
    return 1
  }

  val me01 = (a: Int) => a + 1

  /**
    * 获取连接
    */
  def getConn: Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:mysql://192.168.80.123:3306/portrait?user=root&password=")
    conn
  }

  def main(args: Array[String]): Unit = {
    val a = me(1)
    println(a)
    // val conf = new SparkConf().setAppName("jdbc").setMaster("local[2]")
    //val sc = new SparkContext(conf)
    //    println(sc.parallelize(List("lijie lijie hello", "aaa bbb aaa", "aaa bbb aaa ccc")).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2).mapPartitionsWithIndex(func).collect().toBuffer)
    // sc.parallelize(List("lijie lijie hello", "aaa bbb aaa", "aaa bbb aaa ccc")).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2).foreachPartition(persistData(_))
    //    val query = new JdbcRDD(sc, getConn _, "select * from jdbc_spark where id between ? and ? ", 1, 5, 2, rs => {
    //      (rs.getInt(1), rs.getString(2), rs.getInt(3))
    //    })
    //    println(query.count())
    //    for (i <- query.collect) {
    //      println("id:" + i._1 + "   w:" + i._2 + "   c:" + i._3)
    //    }
  }
}


