package grimoire.ml.statistics

import grimoire.configuration.param.{HasCache, HasInputCols}
import grimoire.ml.configuration.param.HasCorrelationMethod
import grimoire.ml.feature.transform.DataFrameVectorAssemblerSpell
import grimoire.ml.linalg.{LabeledDenseMatrix, LabeledMatrix, LabeledSparseMatrix}
import grimoire.transform.Spell
import org.apache.spark.sql.DataFrame
import grimoire.Implicits._
import grimoire.spark.transform.dataframe.DataFrameSelectToRDDSpell
import org.apache.spark.ml.linalg.{DenseMatrix, SparseMatrix, Vector}
import play.api.libs.json.JsValue

/**
  * Created by caphael on 2017/7/19.
  */
class DataFrameCorrelationsSpell extends Spell[DataFrame,LabeledMatrix] with HasInputCols with HasCorrelationMethod
  with HasCache {
  val vecs = DataFrameVectorAssemblerSpell()
  val df2rdd = DataFrameSelectToRDDSpell[Vector]()
  val corrs = CorrelationsSpell()
  final val tmpOutputCol = "vector4corr"

  override def setup(dat: DataFrame): Boolean = {
    vecs.setInputCols($(inputCols)).setOutputCol(tmpOutputCol)
    df2rdd.setInputCol(tmpOutputCol)
    corrs
      .setCorrelationMethod($(corMethod))
      .setCache($(cache))

    super.setup(dat)
  }

  override def transformImpl(dat: DataFrame): LabeledMatrix = {
    vecs ~ df2rdd ~ corrs transform dat asML match {
      case m:DenseMatrix => LabeledDenseMatrix(m,$(inputCols).toArray,$(inputCols).toArray)
      case m:SparseMatrix => LabeledSparseMatrix(m,$(inputCols).toArray,$(inputCols).toArray)
    }
  }
}

object DataFrameCorrelationsSpell{
  def apply(json: JsValue= """{}"""): DataFrameCorrelationsSpell = new DataFrameCorrelationsSpell().parseJson(json)
}