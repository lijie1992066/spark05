package cn.lijie.business

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/26 
  * Time: 17:45  
  */
object JDBCTestNew {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("jdbc").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val nyr = "2015-01-01"

    val b_str = nyr + " 00:00:00"
    val e_str = nyr + " 23:59:59"

    val strSql1 = s" select  count(1) from table1 t where t.optime  >= '$b_str' and  t.optime <> ? and t.optime <> ?  "

    val strSql2 = " select t.字段1,t.optime from " +
      s" (select t.*,@rownum:=@rownum+1 AS rownum from table1 t,(SELECT @rownum:=0) a where  t.optime  >= '$b_str') t " +
      " where t.rownum >= ? and t.rownum <= ?"

    //该执行返回我们指定时间内符合条件数据量的总条数
    val count_rdd_lrb = new JdbcRDD(sc, () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.80.123:3306/portrait?user=root&password=")
    },
      strSql1,
      0, 0, 1,
      x => x
    ).map { x => x.getInt(1) }

    val count = count_rdd_lrb.collect()(0)

    //启动新的jdbcRdd 根据数据大小 分100个区，取出数据转换成RDD
    val rdd_lrb = new JdbcRDD(sc, () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.80.123:3306/portrait?user=root&password=")
    },
      strSql2,
      1, count, 100,
      x => x
    ) //.map { 处理函数(使用getString,getLong等方法取出数据，处理后放入元组或者case class中返回)}
  }
}
