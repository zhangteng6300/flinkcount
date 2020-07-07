import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * Created by 86977 on 2020/5/29.
  */
class Conn2Mysql(url:String,user:String,pwd:String) extends RichSinkFunction[(String,Integer)]{
var conn:Connection=_
  var pres:PreparedStatement=_
  var sql="replace into fl_wc(word,num) values(?,?)";
  override def invoke(value: (String, Integer)): Unit ={
    pres.setString(1,value._1);
    pres.setInt(2, value._2);
    pres.executeUpdate();
    System.out.println("values ：" + value._1 + "--" + value._2);
  }
  override def open(parameters: Configuration): Unit ={
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(url,user,pwd);
    pres=conn.prepareStatement(sql);
    super.close()
  }
  override def close(): Unit ={
    pres.close();
    conn.close();
  }
}
