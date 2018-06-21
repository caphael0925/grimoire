package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasWithMean, HasWithStd}
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-22.
  */
class DataFrameStandardScalerSpell extends DataFrameSpell with HasInputCol
  with HasOutputCol with HasWithMean with HasWithStd{
  val scaler = new StandardScaler()

  override def setup(dat: DataFrame): Boolean = {
    scaler
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setWithStd($(withStd))
      .setWithMean($(withMean))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    val model = scaler.fit(dat)
    model.transform(dat)
  }
}

object DataFrameStandardScalerSpell{
  def apply(json: JsValue="""{}"""): DataFrameStandardScalerSpell =
    new DataFrameStandardScalerSpell().parseJson(json)
}