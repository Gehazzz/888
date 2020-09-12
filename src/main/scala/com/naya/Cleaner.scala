package com.naya

import java.io.File

object Cleaner {
  implicit def clearDirectory(path: String): Unit = {
    deleteOnlyFiles(new File(path))
  }

  private def deleteOnlyFiles(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteOnlyFiles)
    } else {
      if (file.exists && !file.delete) {
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
    }
  }
}
