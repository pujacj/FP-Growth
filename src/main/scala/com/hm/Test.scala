package com.hm
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD
import org.dmg.pmml.True

/**
  * Created by pooja on 12/4/17.
  */

object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SalesData").setMaster("local")
    val sc = new SparkContext(conf)
    var data = sc.textFile("/home/pooja/Desktop/latsales.csv")
    val kv = data.map(i => {
      val k = i.split(',')
      (k(0), k(1))
    })
    kv.groupByKey
    //kv.groupByKey.map(_.2.mkString(",")).collect.foreach(println)
    //kv.groupByKey.flatMap(_.2.mkString(",")).collect.foreach(println)
    kv.groupByKey.map(i=>i._2.mkString(",")).collect.foreach(println)

    val dataSet = kv.groupByKey.map(i => i._2.toArray)
    //val dataSet:RDD[Array[String]] = kv.groupByKey.map(i => i._2.toArray).union(sc.parallelize(Array("044HS106-11")))

    val fpg = new FPGrowth().setMinSupport(0.0).setNumPartitions(10)
    val model = fpg.run(dataSet)


    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

//    model.freqItemsets.collect().foreach { itemset =>
//      println(itemset.items.size)
//    }

    val minConfidence = 0.0
//    time{model.generateAssociationRules(minConfidence).collect().sortBy(rule => rule.confidence).foreach { rule =>
//
//      if(rule.antecedent.contains("milk") && (rule.antecedent.size ==1))
//        println(
//          rule.antecedent.mkString("[", ",", "]")
//            + "," + rule.consequent.mkString("[", ",", "]")
//            + "," + rule.confidence)
//
//    }}
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + "," + rule.consequent .mkString("[", ",", "]")
          + "," + rule.confidence)
    }
//model.generateAssociationRules(minConfidence).toJavaRDD().repartition(1).
    //
    // ("target/items")

println("-----------printing only singleton antecedant rules-------------------------------------")

    model.generateAssociationRules(minConfidence).collect().foreach { rule => if(rule.antecedent.size ==1)
      println(
        rule.antecedent.mkString("[", ",", "]")
          + "," +
          "" + rule.consequent .mkString("[", ",", "]")
          + "," + rule.confidence)
    }
    println("-----------printing rules with 2 items in antecedant-------------------------------------")

    model.generateAssociationRules(minConfidence).collect().foreach { rule => if(rule.antecedent.size ==2)
      println(
        rule.antecedent.mkString("[", ",", "]")
          + "," + rule.consequent .mkString("[", ",", "]")
          + "," + rule.confidence)
    }
    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println("Elapsed time: " + (t1 - t0) + "ms")
      result
    }

  }

}
