package grimoire.ml.evaluate

import grimoire.ml.configuration.param.{HasLabelCol, HasPredictionCol}
import grimoire.ml.evaluate.result.RegressionMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-19.
  */
class DataFrameRegressionMetricsSpell extends Spell[DataFrame,RegressionMetricsResult]
  with HasLabelCol with  HasPredictionCol{

  override def setup(dat:DataFrame): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat:DataFrame ): RegressionMetricsResult = {
    val dt = dat.select($(labelCol),$(predictionCol)).rdd.map{
      case Row(a:Double,b:Double) =>
        (a,b)
    }
    val mc = new RegressionMetrics(dt)
    RegressionMetricsResult(
      mc.explainedVariance,
      mc.meanAbsoluteError,
      mc.meanSquaredError,
      mc.r2,
      mc.rootMeanSquaredError
    )
  }
}

object DataFrameRegressionMetricsSpell{
  def apply(json: JsValue="""{}"""): DataFrameRegressionMetricsSpell =
    new DataFrameRegressionMetricsSpell().parseJson(json)
}


