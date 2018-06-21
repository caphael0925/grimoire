package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasGaps, HasMintokenLength, HasPattern, HasToLowercase}
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-21.
  */
class DataFrameRegexTokenizerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasPattern with HasGaps with HasMintokenLength with HasToLowercase{
  val regexTokenizer = new RegexTokenizer()

  override def setup(dat: DataFrame): Boolean = {
    regexTokenizer
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setPattern($(pattern))
      .setGaps($(gaps))
      .setMinTokenLength($(minTokenLength))
      .setToLowercase($(toLowercase))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    regexTokenizer.transform(dat)
  }

}
object DataFrameRegexTokenizerSpell{
  def apply(json: JsValue="""{}"""): DataFrameRegexTokenizerSpell
  = new DataFrameRegexTokenizerSpell().parseJson(json)
}