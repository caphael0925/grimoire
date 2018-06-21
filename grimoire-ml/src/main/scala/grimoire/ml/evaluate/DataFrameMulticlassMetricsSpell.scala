package grimoire.ml.evaluate

import grimoire.ml.configuration.param.{HasLabelCol, HasPredictionCol}
import grimoire.ml.evaluate.result.MulticlassMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-19.
  */
class DataFrameMulticlassMetricsSpell extends Spell[DataFrame,MulticlassMetricsResult]
  with HasLabelCol with  HasPredictionCol{

  override def setup(dat:DataFrame): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): MulticlassMetricsResult = {
    val dt = dat.select($(predictionCol),$(labelCol)).rdd.map{
      case Row(a:Double,b:Double) =>
      (a,b)
    }
    val mc = new MulticlassMetrics(dt)
    MulticlassMetricsResult(
      mc.confusionMatrix,
      mc.accuracy,
      mc.weightedFMeasure,
      mc.weightedPrecision,
      mc.weightedRecall,
      mc.weightedTruePositiveRate,
      mc.weightedFalsePositiveRate
    )
  }
}

object DataFrameMulticlassMetricsSpell{
  def apply(json: JsValue="""{}"""): DataFrameMulticlassMetricsSpell =
    new DataFrameMulticlassMetricsSpell().parseJson(json)
}
