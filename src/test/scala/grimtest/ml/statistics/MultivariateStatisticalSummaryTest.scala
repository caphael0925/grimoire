package grimtest.ml.statistics

import grimoire.ml.feature.transform.RDDSeqDoubleToVectorSpell
import grimoire.ml.parsing.transform.RDDDoubleSeqParsingSpell
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.Implicits._
import grimoire.ml.linalg.transform.ResultToLabeledMatrixSpell
import grimoire.ml.statistics.{DataFrameCorrelationsSpell, DataFrameMultivariateStatisticalSummarySpell, MultivariateStatisticalSummarySpell}
import grimoire.spark.source.dataframe.CSVSource
import grimoire.zeppelin.transform.{ZeppelinLabeledMatrixSpell, ZeppelinSummaryResultSpell}

/**
  * Created by sjc505 on 17-7-12.
  */
object MultivariateStatisticalSummaryTest {
  globalSpark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().config("spark.yarn.dist.files","conf/hive-site.xml").getOrCreate()

//  val dt =
//    TextFileRDDSource("""{"InputPath":"data/svd.txt"}""").
//      cast(RDDDoubleSeqParsingSpell()).
//      cast(RDDSeqDoubleToVectorSpell())
//  val mss=dt.cast(MultivariateStatisticalSummarySpell())

  val df = CSVSource("""{"InputPath":"data/iris.csv","Schema":"f1 double,f2 double,f3 double,f4 double,label string"}""")

//  val sum= df.cast(DataFrameMultivariateStatisticalSummarySpell().setInputCols(Seq("f1","f2","f3","f4"))).
//    cast(ResultToLabeledMatrixSpell().setColLabels(Seq("f1","f2","f3","f4")).setRowLabels(Seq("max","min","mean","normL1","normL2","numNonzeros","variance")).setTransposed(true))

  val sum= df.cast(DataFrameMultivariateStatisticalSummarySpell().setInputCols(Seq("f1","f2","f3","f4"))
    .setColLabels(Seq("f1","f2","f3","f4")).setRowLabels(Seq("count","max","min","mean","normL1","normL2","numNonzeros","variance")).setTransposed(true))
  sum.cast(ZeppelinLabeledMatrixSpell().setThreshold(6.0)).conjure

}
