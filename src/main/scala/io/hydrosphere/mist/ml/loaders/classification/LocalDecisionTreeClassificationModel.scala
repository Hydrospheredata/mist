package io.hydrosphere.mist.ml.loaders.classification

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.tree._

object LocalDecisionTreeClassificationModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): DecisionTreeClassificationModel = {
    println(s"DTREE with: $data")
    createTree(metadata, data)
  }

  def createTree(metadata: Metadata, data: Map[String, Any]): DecisionTreeClassificationModel = {
    val ctor = classOf[DecisionTreeClassificationModel].getDeclaredConstructor(classOf[String], classOf[Node], classOf[Int], classOf[Int])
    ctor.setAccessible(true)
    val inst = ctor.newInstance(
      metadata.uid,
      createNode(0, metadata, data),
      metadata.numFeatures.get.asInstanceOf[java.lang.Integer],
      metadata.numClasses.get.asInstanceOf[java.lang.Integer]
    )
    inst.setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
  }

  def createNode(nodeId: Int, metadata: Metadata, treeData: Map[String, Any]): Node = {
    val nodeData = treeData(nodeId.toString).asInstanceOf[Map[String, Any]]
    val impurity = createImpurityCalculator(
      metadata.paramMap("impurity").asInstanceOf[String],
      nodeData("impurityStats").asInstanceOf[List[Double]].to[Array]
    )

    if (isInternal(nodeData)) {
      val ctor = classOf[InternalNode].getDeclaredConstructor(
        classOf[Double],
        classOf[Double],
        classOf[Double],
        classOf[Node],
        classOf[Node],
        classOf[Split],
        impurity.getClass.getSuperclass
      )
      ctor.newInstance(
        nodeData("prediction").asInstanceOf[java.lang.Double],
        nodeData("impurity").asInstanceOf[java.lang.Double],
        nodeData("gain").asInstanceOf[java.lang.Double],
        createNode(nodeData("leftChild").asInstanceOf[java.lang.Integer], metadata, treeData),
        createNode(nodeData("rightChild").asInstanceOf[java.lang.Integer], metadata, treeData),
        createSplit(nodeData("split").asInstanceOf[Map[String, Any]]),
        impurity
      )
    } else {
      val ctor = classOf[LeafNode].getDeclaredConstructor(
        classOf[Double],
        classOf[Double],
        impurity.getClass.getSuperclass
      )
      ctor.newInstance(
        nodeData("prediction").asInstanceOf[java.lang.Double],
        nodeData("impurity").asInstanceOf[java.lang.Double],
        impurity
      )
    }
  }

  def isInternal(nodeData: Map[String, Any]) : Boolean =
    (nodeData("leftChild").asInstanceOf[java.lang.Integer] == -1) && (nodeData("rightChild").asInstanceOf[java.lang.Integer] == -1)

  def createImpurityCalculator(impurity: String, stats: Array[Double]): Object = {
    val className = impurity match {
      case "gini" => "org.apache.spark.mllib.tree.impurity.GiniCalculator"
      case "entropy" => "org.apache.spark.mllib.tree.impurity.EntropyCalculator"
      case "variance" => "org.apache.spark.mllib.tree.impurity.VarianceCalculator"
      case _ =>
        throw new IllegalArgumentException(s"ImpurityCalculator builder did not recognize impurity type: $impurity")
    }
    val ctor = Class.forName(className).getDeclaredConstructor(classOf[Array[Double]])
    ctor.setAccessible(true)
    ctor.newInstance(stats).asInstanceOf[Object]
  }

  def createSplit(data: Map[String, Any]): Split = {
    data("leftCategoriesOrThreshold") match {
      case doubles: Array[Double] =>
        val ctor = classOf[CategoricalSplit].getDeclaredConstructor(classOf[Int], classOf[Array[Double]], classOf[Int])
        ctor.setAccessible(true)
        ctor.newInstance(
          data("featureIndex").asInstanceOf[java.lang.Integer],
          doubles, data("numCategories").asInstanceOf[java.lang.Integer]
        )
      case threshold: java.lang.Double =>
        val ctor = classOf[ContinuousSplit].getDeclaredConstructor(classOf[Int], classOf[Double])
        ctor.setAccessible(true)
        ctor.newInstance(data("featureIndex").asInstanceOf[java.lang.Integer], threshold)
      case _ =>
        throw new IllegalArgumentException(s"Unknown split $data")
    }
  }
}
