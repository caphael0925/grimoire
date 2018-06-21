package grimoire.ml.feature.transform

import org.apache.spark.ml.feature.CountVectorizer
import grimoire.Implicits._
import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.{HasBinary, HasMinDF, HasMinTF, HasVocabSize}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue


/**
  * Created by sjc505 on 17-6-20.
  */

class DataFrameCountVectorzerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasVocabSize with HasMinDF with HasMinTF with HasBinary{

  val cv: CountVectorizer = new CountVectorizer()
  override def setup(dat: DataFrame): Boolean = {
    cv
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setVocabSize($(vocabSize))
      .setMinDF($(minDF))
      .setMinTF($(minTF))
      .setBinary($(binary))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val model = cv.fit(dat)
    model.transform(dat)
  }
}


object DataFrameCountVectorzerSpell{
  def apply(json: JsValue="""{}"""): DataFrameCountVectorzerSpell =
    new DataFrameCountVectorzerSpell().parseJson(json)
}