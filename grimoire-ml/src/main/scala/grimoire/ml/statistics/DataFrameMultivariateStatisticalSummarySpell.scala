package grimoire.ml.statistics

import grimoire.ml.statistics.result.MultivariateStatisticalSummaryResult
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import play.api.libs.json.JsValue
import grimoire.Implicits._
import grimoire.configuration.param.HasCache
import grimoire.ml.configuration.param._
import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
import grimoire.ml.linalg.{LabeledDenseMatrix, LabeledMatrix, LabeledSparseMatrix}
import grimoire.spark.transform.dataframe.DataFrameSelectToRDDSpell
import org.apache.spark.ml.linalg.{DenseMatrix, SparseMatrix, Vector}

import scala.language.postfixOps

/**
  * Created by sjc505 on 17-7-25.
  */
class DataFrameMultivariateStatisticalSummarySpell extends Spell[DataFrame,LabeledMatrix]
  with HasInputCols with HasRowLabels with HasColLabels with HasTransposed with HasCache
  with HasNumCols with HasNumRows{
  val vecs = DataFrameVectorAssemblerSpell()
  val df2rdd = DataFrameSelectToRDDSpell[Vector]()
  val sum = MultivariateStatisticalSummarySpell()

  final val tmpOutputCol = "vector4sum"

  override def setup(dat: DataFrame): Boolean = {
    vecs.setInputCols($(inputCols)).setOutputCol(tmpOutputCol)
    df2rdd.setInputCol(tmpOutputCol)
    sum
      .setRowLabels($(rowLabels))
      .setColLabels($(colLabels))
      .setTransposed($(transposed))
      .setCache($(cache))
      .setNumCols($(numCols))
      .setNumRows($(numRows))

    super.setup(dat)
  }
  override def transformImpl(dat: DataFrame): LabeledMatrix = {
    vecs ~ df2rdd ~ sum transform dat
  }
}

object DataFrameMultivariateStatisticalSummarySpell{
  def apply(json: JsValue="""{}"""): DataFrameMultivariateStatisticalSummarySpell =
    new DataFrameMultivariateStatisticalSummarySpell().parseJson(json)
}