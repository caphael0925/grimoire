package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import grimoire.ml.configuration.param.{HasBinary, HasNumFeatures}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits.jstr2JsValue

/**
  * Created by caphael on 2017/1/10.
  */
class DataFrameTFVectorizeSpell extends DataFrameSpell
  with HasInputCol with HasOutputCol with HasNumFeatures with HasBinary{

  override def transformImpl(dat: DataFrame): DataFrame = {

    val hashingTF = new HashingTF()
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setNumFeatures($(numFeatures))
      .setBinary($(binary))
    hashingTF.transform(dat)
  }

}

object DataFrameTFVectorizeSpell{
  def apply(json:JsValue=""""""): DataFrameTFVectorizeSpell =
    new DataFrameTFVectorizeSpell().parseJson(json)
}