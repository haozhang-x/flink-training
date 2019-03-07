package wordcount

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 2019/03/06
  * flink wordcount
  *
  * @author zhanghao
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    //创建env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从localhost:9999 接收数据
    val text = env.socketTextStream("localhost", 9999)

    //对接收过来的每一行数据按照逗号进行分割，然后根据单词进行统计，并将结果输出出来
    text.flatMap(_.split(",")).map(Word(_, 1))
      .keyBy("word").timeWindow(Time.seconds(2)).sum("count")
      .print.setParallelism(1)

    //执行job
    env.execute("WordCount")


  }

  //单词case class
  case class Word(word: String, count: Long)

}
