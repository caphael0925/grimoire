package grimoire.ml.feature.transform


import grimoire.ml.configuration.param._
import org.apache.spark.ml.feature.Word2Vec
import grimoire.Implicits._
import grimoire.configuration.param.{HasInputCol, HasMaxIter, HasOutputCol}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue


/**
  * Created by sjc505 on 17-6-20.
  */
class DataFrameWord2VecSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasVectorSize with HasMinCount with HasMaxIter
  with HasSeed with HasStepSize with HasWindowSize with HasMaxSentenceLength{


  val word2Vec = new Word2Vec()

  override def setup(dat: DataFrame): Boolean = {
    word2Vec
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setVectorSize($(vectorsize))
      .setMinCount($(mincount))
      .setMaxIter($(maxIter))
      .setMaxSentenceLength($(maxSentenceLength))
      .setSeed($(seed))
      .setStepSize($(stepSize))
      .setWindowSize($(windowSize))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame= {
    val model = word2Vec.fit(dat)
    model.transform(dat)
  }
}


object DataFrameWord2VecSpell{
  def apply(json: JsValue="""{}"""): DataFrameWord2VecSpell =
    new DataFrameWord2VecSpell().parseJson(json)
}