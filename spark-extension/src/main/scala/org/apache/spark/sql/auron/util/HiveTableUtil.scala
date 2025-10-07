package org.apache.spark.sql.auron.util

import org.apache.hadoop.mapred.InputFormat

object HiveTableUtil {
  def getFileFormat(inputFormatClass: Class[_ <: InputFormat[_, _]]): String = {
    if (inputFormatClass.getSimpleName.equalsIgnoreCase("OrcInputFormat")) {
      "orc"
    } else if (inputFormatClass.getSimpleName.equalsIgnoreCase("MapredParquetInputFormat")) {
      "parquet"
    } else {
      "other"
    }
  }

}
