package grimoire.ml.classify.transform

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.{HasMaxIter, HasRegParam}
import grimoire.ml.configuration.param._
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame

/**
  * Created by sjc505 on 17-6-26.
  */
class DataFrameLogisticRegressionTrainSpell
  extends Spell[DataFrame,LogisticRegressionModel]
  with HasMaxIter with HasRegParam with HasElasticNetParam with HasFamily
    with HasLabelCol with HasFeaturesCol with HasRawPredictionCol with HasFitIntercept
    with HasStandardization with HasThresholds with HasThreshold with HasTol
    with HasWeightCol with HasProbabilityCol with HasPredictionCol{

  val mlr = new LogisticRegression()

  override def setup(dat: DataFrame ): Boolean = {
    mlr
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setWeightCol($(weightCol))
      .setProbabilityCol($(probabilityCol))
      .setPredictionCol($(predictionCol))
      .setRawPredictionCol($(rawPredictionCol))
      .setMaxIter($(maxIter))
      .setRegParam($(regParam))
      .setElasticNetParam($(elasticNetParam))
      .setFamily($(family))
      .setFitIntercept($(fitIntercept))
      .setStandardization($(standardization))
      .setThreshold($(threshold))
      .setThresholds($(thresholds).toArray)
      .setTol($(tol))
    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): LogisticRegressionModel = {
    mlr.fit(dat)
  }
}

object DataFrameLogisticRegressionTrainSpell {
  def apply(json: JsValue ="""{}"""): DataFrameLogisticRegressionTrainSpell =
    new DataFrameLogisticRegressionTrainSpell().parseJson(json)
}