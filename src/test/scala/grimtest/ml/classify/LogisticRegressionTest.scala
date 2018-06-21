package grimtest.ml.classify

/**
  * Created by sjc505 on 17-6-26.
  */
object LogisticRegressionTest {

  import grimoire.ml.classify.transform.{DataFrameLogisticRegressionPredictSpell,  DataFrameLogisticRegressionTrainSpell}
  import grimoire.spark.source.dataframe.CSVSource
  import grimoire.spark.globalSpark
  import org.apache.spark.sql.SparkSession
  import grimoire.Implicits._
  import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell

  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()
  val df = CSVSource("""{"InputPath":"data/chisq.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label double"}""").
    cast(DataFrameVectorAssemblerSpell().setInputCols(Seq("f1","f2","f3","f4")).setOutputCol("features"))


  val mod = df.cast(DataFrameLogisticRegressionTrainSpell().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    .setFamily("multinomial").setLabelCol("label").setFeaturesCol("features"))
 // mod.cast(ModelTarget("""{"OutputPath":"model/logisticRegression","Overwrite":true}"""))

  val pre = (df :+ mod).
    cast(DataFrameLogisticRegressionPredictSpell().setFeaturesCol("features").setPredictionCol("pre"))

}
