package grimoire.ml.target

import grimoire.target.TargetToFile
import org.apache.spark.ml.util.MLWritable

/**
  * Created by caphael on 2017/3/27.
  */
class MLWritableTarget extends TargetToFile[MLWritable]{
  override def transformImpl(dat: MLWritable): Unit = {
    val writer = if ($(overwrite)) dat.write.overwrite() else dat.write
    writer.save($(outputPath))
  }
}
