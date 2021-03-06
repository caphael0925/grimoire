package grimtest.ml.feature

import grimoire.ml.feature.transform.DataFrameNormalizerSpell
import grimoire.Implicits._
import grimoire.ml.feature.transform.{DataFrameStopWordsRemoverSpell, DataFrameWord2VecSpell}
import grimoire.nlp.transform.DataFrameSegmentSpell
import grimoire.spark.globalSpark
import org.apache.spark.sql.SparkSession
import grimoire.spark.source.rdd.TextFileRDDSource
import grimoire.spark.transform.rdd.{RDDLineSplitSpell, StringSeqRDDToDFSpell}


/**
  * Created by sjc505 on 17-6-22.
  */
object NormalizerTest {
  globalSpark = SparkSession.builder().master("local").appName("test").getOrCreate()

  val norm=
    TextFileRDDSource("""{"InputPath":"data/test.txt"}""").
      cast(RDDLineSplitSpell("""{"Separator":"\\s+","NumField":2}""")).
      cast(StringSeqRDDToDFSpell("""{"Schema":"id int,content string"}""")).
      cast(DataFrameSegmentSpell("""{"InputCol":"content","OutputCol":"segmented"}""")).
      cast(DataFrameStopWordsRemoverSpell("""{"InputCol":"segmented","OutputCol":"keyword"}""")).
      cast(DataFrameWord2VecSpell("""{"InputCol":"segmented","OutputCol":"wordvector"}""").setMinCount(0).setVectorSize(3)).
      cast(DataFrameNormalizerSpell().setInputCol("wordvector").setOutputCol("norm").setP(1.0))

}

