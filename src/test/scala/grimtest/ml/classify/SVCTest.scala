package grimtest.ml.classify

/**
  * Created by caphael on 2017/3/23.
  */
object SVCTest extends App{
  import grimoire.Implicits._
  import grimoire.spark.globalSpark
  import grimoire.ml.transform.{GenericDataFrameMappingSpell, StringLongMapKeeperReverseSpell}
  import org.apache.spark.sql.SparkSession
  import grimoire.ml.transform.StringLabelIndexedSpell
  import grimoire.ml.feature.transform.DataFrameMLLabeledPointSpell
  import grimoire.ml.classify.transform.SVMTrainSpell
  import grimoire.ml.target.ModelTarget
  import grimoire.spark.source.rdd.TextFileRDDSource
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.spark.transform.dataframe.DataFrameFilterSpell
  import grimoire.ml.source.StringLongMapKeeperSource

  import grimoire.ml.classify.source.SVMModelSource
  import grimoire.ml.classify.transform.DataFrameSVMPredictSpell
  import grimoire.ml.feature.transform.DataFrameVectorSlicerSpell

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val df = CSVSource().setInputPath("data/iris.csv").setSchema("f1 double,f2 double,f3 double,f4 double,label string").
    cast(DataFrameFilterSpell().setFilterExpr("label <> 'setosa'"))


  val labmap = df.cast(new StringLabelIndexedSpell().setInputCol("label"))
//  labmap.cast(StringLongMapKeeperTarget({}""").setOutputPath("data/mapkeeper").setOverwrite(true))
//  val labmap = StringLongMapKeeperSource("""{}""").setInputPath("data/mapkeeper")


  val mod = (df :+ labmap).cast(new SVMTrainSpell().setLabelCol("label").setNumIter(10).setFeatureCols("*"))
//  mod.cast(ModelTarget("""{"OutputPath":"model/svm","Overwrite":true}"""))
//  val mod = SVMModelSource("""{}""").setInputPath("model/svm")

  val pred = (df :+ mod).cast(new DataFrameSVMPredictSpell().setFeatureCols(Seq("f1","f2","f3","f4")).setOutputCol("pred"))

}
