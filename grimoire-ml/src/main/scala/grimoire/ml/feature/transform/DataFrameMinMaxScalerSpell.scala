package grimoire.ml.feature.transform

import grimoire.configuration.param.{HasInputCol, HasOutputCol}
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param.{HasMax, HasMin}
import grimoire.spark.transform.dataframe.DataFrameSpell
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.DataFrame
/**
  * Created by sjc505 on 17-6-22.
  */
class DataFrameMinMaxScalerSpell extends DataFrameSpell with HasInputCol with HasOutputCol
  with HasMin with HasMax{
  val scaler = new MinMaxScaler()

  override def setup(dat: DataFrame): Boolean = {

    scaler
      .setInputCol($(inputCol))
      .setOutputCol($(outputCol))
      .setMax($(max))
      .setMin($(min))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    scaler.fit(dat).transform(dat)
  }
}
object DataFrameMinMaxScalerSpell{
  def apply(json: JsValue="""{}"""): DataFrameMinMaxScalerSpell =
    new DataFrameMinMaxScalerSpell().parseJson(json)
}
