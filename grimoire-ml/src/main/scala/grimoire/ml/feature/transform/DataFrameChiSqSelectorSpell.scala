package grimoire.ml.feature.transform

import grimoire.configuration.param.HasOutputCol
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.ml.configuration.param._
import grimoire.spark.transform.dataframe.DataFrameSpell

/**
  * Created by sjc505 on 17-6-22.
  */
class DataFrameChiSqSelectorSpell extends DataFrameSpell with HasOutputCol
  with HasFeaturesCol with HasLabelCol with HasNumTopFeatures with HasFpr
  with HasFwe with HasFdr with HasPercentile with HasSelectorType{

  val selector = new ChiSqSelector()

  override def setup(dat: DataFrame): Boolean = {
    selector
      .setNumTopFeatures($(numTopFeatures))
      .setFeaturesCol($(featuresCol))
      .setLabelCol($(labelCol))
      .setOutputCol($(outputCol))
      .setFpr($(fpr))
      .setPercentile($(percentile))
      .setSelectorType($(selectorType))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): DataFrame = {
    selector.fit(dat).transform(dat)
  }
}
object DataFrameChiSqSelectorSpell{
  def apply(json: JsValue="""{}"""): DataFrameChiSqSelectorSpell =
    new DataFrameChiSqSelectorSpell().parseJson(json)
}