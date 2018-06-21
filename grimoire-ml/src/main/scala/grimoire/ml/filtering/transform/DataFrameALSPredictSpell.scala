package grimoire.ml.filtering.transform

import grimoire.configuration.param.HasOutputCol
import grimoire.dataset.&
import grimoire.ml.classify.transform.DataFrameLogisticRegressionPredictSpell
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._

/**
  * Created by sjc505 on 17-6-29.
  */
class DataFrameALSPredictSpell
  extends Spell[DataFrame & ALSModel,DataFrame]
  with HasOutputCol with HasFeaturesCol with HasPredictionCol
    with HasUserCol with HasItemCol with HasRatingCol{
  val als = new ALS()

  override def setup(dat: DataFrame & ALSModel): Boolean = {
    als
      .setUserCol($(userCol))
      .setItemCol($(itemCol))
      .setRatingCol($(ratingCol))
      .setPredictionCol($(predictionCol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame & ALSModel): DataFrame = {
    val vec = als.fit(dat._1).transform(dat._1)
    dat._2.transform(vec)
  }
}

object DataFrameALSPredictSpell{
  def apply(json: JsValue="""{}"""): DataFrameALSPredictSpell =
    new DataFrameALSPredictSpell().parseJson(json)
}

