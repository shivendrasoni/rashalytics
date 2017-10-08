package org.vikings.hackathons.models

import java.sql.Time

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{PolynomialExpansion, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.vikings.hackathons.db.DBClient

object ScoreModel {
  def main(args: Array[String]): Unit = {
    val dbClient = new DBClient
    val allScore: List[(Double, Double, Double)] = dbClient.getAllScore

    val conf = new SparkConf().setAppName("Model Generation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqLContext = new SQLContext(sc)

    val data = sqLContext.createDataFrame(allScore).toDF("driverId", "hour", "score")

    val Array(trainDF, testDF) = data.randomSplit(Array(0.75,0.25))
//    trainDF.show(5)

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("driverId", "hour"))
      .setOutputCol("features")

    val polynomialExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    val lr = new LinearRegression()
      .setLabelCol("score")
      .setFeaturesCol("polyFeatures")


    val pipeline = new Pipeline().setStages(Array(vectorAssembler, polynomialExpansion, lr))

    val model = pipeline.fit(trainDF)


    model.write.overwrite.save("/tmp/scoreModel")

    val newList = List((1, 3), (2, 4))
    val tDF = sqLContext.createDataFrame(newList).toDF("driverId", "hour")
    tDF.show()
    model.transform(tDF).show

    val holdout = model.transform(testDF).select("prediction", "score")

    // have to do a type conversion for RegressionMetrics
    val rm = new RegressionMetrics(holdout.rdd.map(x =>
      (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
    println("MSE: " + rm.meanSquaredError)
    println("R Squared: " + rm.r2)
    println("Explained Variance: " + rm.explainedVariance + "\n")
  }
}
