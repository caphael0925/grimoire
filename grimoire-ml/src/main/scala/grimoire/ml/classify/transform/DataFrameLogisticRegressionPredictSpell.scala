package grimoire.ml.classify.transform

import grimoire.configuration.param.HasOutputCol
import grimoire.dataset.&
import grimoire.ml.configuration.param.{HasFeatureCols, HasFeaturesCol, HasPredictionCol}
import grimoire.transform.Spell
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-26.
  */
class DataFrameLogisticRegressionPredictSpell
  extends Spell[DataFrame & LogisticRegressionModel,DataFrame]
  with HasOutputCol with HasFeaturesCol with HasPredictionCol{
  val lr = new LogisticRegression()

  override def setup(dat: DataFrame & LogisticRegressionModel): Boolean = {
    lr
      .setFeaturesCol($(featuresCol))
      .setPredictionCol($(predictionCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame & LogisticRegressionModel): DataFrame = {
    val vec = lr.fit(dat._1).transform(dat._1)
    dat._2.transform(vec)
  }
}

object DataFrameLogisticRegressionPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameLogisticRegressionPredictSpell =
    new DataFrameLogisticRegressionPredictSpell().parseJson(json)
}

