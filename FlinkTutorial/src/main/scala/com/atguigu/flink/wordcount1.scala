package com.atguigu.flink

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment, _}

/**
  * Author: Wmd
  * Date: 2019/7/29 17:00
  */
object wordcount1 { //批处理

  //批处理
  def main(args: Array[String]): Unit = {


    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPath = "G:\\Software\\ScalaWorkspace\\gitforidea\\flink\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    //通过路径进行传输
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    // 分词之后，对单词进行groupby分组，然后用sum进行聚合
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_, 1))
      .groupBy(0)   //元素的下标
      .sum(1)

    // 打印输出
    wordCountDS.print()


  }

}
