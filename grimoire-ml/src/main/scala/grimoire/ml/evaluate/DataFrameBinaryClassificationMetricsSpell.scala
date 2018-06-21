package grimoire.ml.evaluate

import grimoire.ml.configuration.param.{HasLabelCol, HasNumBins, HasPredictionCol}
import grimoire.ml.evaluate.result.BinaryClassificationMetricsResult
import grimoire.transform.Spell
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, Row}
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-7-26.
  */
class DataFrameBinaryClassificationMetricsSpell  extends Spell[DataFrame,BinaryClassificationMetricsResult]
  with HasLabelCol with  HasPredictionCol {

  override def setup(dat:DataFrame): Boolean = {
    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): BinaryClassificationMetricsResult = {
    val dt = dat.select($(labelCol),$(predictionCol)).rdd.map{
      case Row(a:Double,b:Double) =>
        (a,b)
    }
    val bcm = new BinaryClassificationMetrics(dt)

    BinaryClassificationMetricsResult(
      bcm.fMeasureByThreshold,
      bcm.precisionByThreshold,
      bcm.recallByThreshold,
      bcm.roc,
      bcm.areaUnderPR,
      bcm.areaUnderROC,
      bcm.thresholds
    )
  }
}

object DataFrameBinaryClassificationMetricsSpell{
  def apply(json: JsValue="""{}"""): DataFrameBinaryClassificationMetricsSpell =
    new DataFrameBinaryClassificationMetricsSpell().parseJson(json)
}

