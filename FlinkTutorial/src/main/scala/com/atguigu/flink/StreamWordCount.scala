package com.atguigu.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Author: Wmd
  * Date: 2019/7/31 12:41
  */
object StreamWordCount {  //流处理
  def main(args: Array[String]): Unit = {


    //从外部命令中获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")


    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //通过socket 进行数据传输
    val textDstream: DataStream[String] = env.socketTextStream(host,port)

    //flapMap 和map的运算需要引用隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] = textDstream.flatMap(_.split(" ")).filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)  //对第二个元素进行sum求和

    dataStream.print().setParallelism(1) //并行度为1

    //启动executor 执行任务
    env.execute("Socket stream word count")

  }

 /* def main(args: Array[String]): Unit = {
    // 从外部命令中获取参数
    val params: ParameterTool =  ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream(host, port)

    // flatMap和Map需要引用的隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] = textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    dataStream.print()//.setParallelism(1)
    //setParallelism(1)  设置并行度

    // 启动executor，执行任务
    env.execute("Socket stream word count")
  }*/


}
