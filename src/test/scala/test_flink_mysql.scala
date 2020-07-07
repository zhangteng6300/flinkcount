import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.types.Row

/**
  * Created by 86977 on 2020/5/28.
  */
object test_flink_mysql {
  def main(args: Array[String]): Unit = {
    val  env = ExecutionEnvironment.getExecutionEnvironment
    val  inputMysql=testJDBCRead(env)
    // inputMysql.print()
   // inputMysql.writeAsText("C:\Users\86977\IdeaProjects\flinkcount\src\main\resources\outfile")
    /*
     val inputPath = "C:\\Users\\86977\\IdeaProjects\\flinkcount\\src\\main\\resources\\word"
     val inputDS: DataSet[String] = env.readTextFile(inputPath)
     // 分词之后，对单词进行groupby分组，然后用sum进行聚合
     val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
   */
  }

  def testJDBCRead(env:ExecutionEnvironment):DataSet[Row] = {
    val inputmysql =env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://172.16.9.58:3306/test")
      .setUsername("shtestagentcloud")
      .setPassword("shtestagentcloud123")
      .setQuery("select id,name from test_fl")
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO))
      .finish()
    )
    inputmysql
  }


/*
  def testJDBCWrite(env: ExecutionEnvironment,wordCountDS: AggregateDataSet[(String, Int)]): Unit = {
  // 生成测试数据，由于插入数据需要是Row格式，提前设置为Row
  // 将集合数据转成DataSet
  // 使用JDBCOutputFormat，将数据写入到Mysql
  // 触发执行
    env.execute("Test JDBC  Output")
  }
*/
}
