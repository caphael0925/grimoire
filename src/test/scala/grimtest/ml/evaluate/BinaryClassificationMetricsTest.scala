package grimtest.ml.evaluate

import grimoire.ml.classify.transform.{DataFrameGBTClassifierPredictSpell, DataFrameGBTClassifierTrainSpell}
import grimoire.ml.evaluate.DataFrameBinaryClassificationMetricsSpell
import grimoire.ml.feature.transform.{DataFrameRandomSplitSpell, DataFrameStringIndexerSpell, DataFrameVectorIndexerSpell, TakeDFSpell}
import grimoire.spark.source.dataframe.LibSvmSource
import grimoire.Implicits._
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.zeppelin.transform.ZepplinBinaryClassificationResultSpell
import grimoire.ml.transform.{GenericDataFrameMappingSpell, StringLongMapKeeperReverseSpell}
import grimoire.ml.transform.StringLabelIndexedSpell
import grimoire.ml.target.StringLongMapKeeperTarget
import grimoire.spark.source.dataframe.CSVSource
import grimoire.spark.transform.dataframe.DataFrameFilterSpell
import grimoire.ml.source.StringLongMapKeeperSource
import grimoire.ml.classify.source.SVMModelSource
import grimoire.ml.classify.transform.DataFrameSVMPredictSpell
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}

/**
  * Created by caphael on 2017/7/14.
  */
object BinaryClassificationMetricsTest extends App{

//
//  //  val df = TextFileRDDSource("""{"InputPath":"data/iris.csv"}""").
//  //    cast(RDDLineSplitSpell("""{"Separator":",","NumField":5}""")).
//  //    cast(StringSeqRDDToDFSpell("""{"Schema":"f1 double,f2 double,f3 double,f4 double,label string"}"""))
//
//  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""").setCache(true).
//    cast(DataFrameFilterSpell().setFilterExpr("label <> 'setosa'"))
//  //  df.cache()
//  //  val labmap = df.cast(StringLabelIndexedSpell("""{}""").setInputCol("label"))
//  //  labmap.cast(StringLongMapKeeperTarget("""{}""").setOutputPath("data/mapkeeper").setOverwrite(true))
//  val labmap = StringLongMapKeeperSource("""{}""").setInputPath("data/mapkeeper")
//
//  //  val mod = (df :+ labmap).castToCache(SVMTrainSpell("""{}""").setLabelCol("label").setNumIter(10).setFeatureCols("*"))
//  //  mod.cast(ModelTarget("""{"OutputPath":"model/svm","Overwrite":true}"""))
//
//  val mod = SVMModelSource("""{}""").setInputPath("model/svm")
//
//  val df2 = (df :+ labmap).cast(GenericDataFrameMappingSpell[String,Long]().setInputCol("label").setOutputCol("labelidx"))
//
//  val pred = (df2 :+ mod).cast(DataFrameSVMPredictSpell("""{"FeatureCols":["f1","f2","f3","f4"],"OutputCol":"pred"}"""))
//
//  val toest = pred.conjure.select("pred","labelidx").rdd.map{
//    case row =>
//      (row.getDouble(0),row.getLong(1).toDouble)
//  }
//  val metrics = new MulticlassMetrics(toest)
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val data = LibSvmSource("""{"InputPath":"data/mllib/sample_libsvm_data.txt"}""").
    cast(DataFrameStringIndexerSpell().setInputCol("label").setOutputCol("indexedlabel")).
    cast(DataFrameVectorIndexerSpell().setInputCol("features").setOutputCol("indexedfeatures").setMaxCategories(10))

  val train = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.7)).cast(TakeDFSpell())
  val test = data.cast(DataFrameRandomSplitSpell().setRandomRate(0.3)).cast(TakeDFSpell().setTakeTrain(false))

  val mod = train.cast(DataFrameGBTClassifierTrainSpell().setLabelCol("indexedlabel").setFeaturesCol("indexedfeatures").setMaxIter(10))

  val pre = (test :+ mod).cast(DataFrameGBTClassifierPredictSpell())
//  val ev = pre.cast(DataFrameBinaryClassificationMetricsSpell().setPredictionCol("indexedlabel").setLabelCol("prediction").setNumBins(2))
//
//  ev.cast(ZepplinBinaryClassificationResultSpell()).conjure
}
