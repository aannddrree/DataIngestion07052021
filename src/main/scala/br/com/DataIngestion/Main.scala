package br.com.DataIngestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.testing.memory", "2147480000")
      .appName("TestData")

      .getOrCreate()

    //Simulator Hive Table
    val dfProduto = spark.read.option("delimiter", ";").option("header",true).csv("C:\\tmp\\hdfs\\tb_produto.csv")
    dfProduto.createOrReplaceTempView("tb_produto")
    val dfParametro = spark.read.option("delimiter", ";").option("header",true).csv("C:\\tmp\\hdfs\\tb_parametro.csv")
    dfParametro.createOrReplaceTempView("tb_parametro")

    //Find Data Parameters
    val df2 = spark.sql("select * from tb_parametro")

    //Create File on HDFS
    df2.collect().foreach(x => {
      writeData(x.get(1).toString, x.get(2).toString, x.get(3).toString, spark)
    })
  }

  //Write CSV Files
  def writeData(query: String, fileOutput: String, dirOutput: String, spark: SparkSession): Unit = {
    val dfOutput = spark.sql(query);
    dfOutput.write.csv(dirOutput + "\\" + fileOutput)
   }
}
