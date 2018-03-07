package org.apache.spark.util

import java.net.URL

import _root_.io.hydrosphere.mist.utils.Logger

object SparkClassLoader extends Logger {

  def withURLs(parent: ClassLoader, urls: URL*): ClassLoader = {
     parent match {
       case sparkLoader: MutableURLClassLoader =>
         urls.foreach(sparkLoader.addURL)
         sparkLoader
       case loader =>
         val message = "Parent is not instance of org.apache.spark.MutableURLClassLoader."
         logger.warn(message)
         new MutableURLClassLoader(urls.toArray, loader)
     }
  }

}