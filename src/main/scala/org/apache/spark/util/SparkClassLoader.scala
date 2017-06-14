package org.apache.spark.util

import java.net.URL

import _root_.io.hydrosphere.mist.utils.Logger

object SparkClassLoader extends Logger {

  def withURLs(parent: ClassLoader, urls: URL*): ClassLoader = {
     parent match {
       case sparkLoader: MutableURLClassLoader =>
         urls.foreach(sparkLoader.addURL)
         sparkLoader
         //new MutableURLClassLoader(urls.toArray ++ sparkLoader.getURLs(), sparkLoader)
         //new ChildFirstURLClassLoader(urls.toArray ++ sparkLoader.getURLs(), sparkLoader)
       case _ =>
         val message =
           "Parent is not instance of org.apache.spark.MutableURLClassLoader." +
           " Method withURLs didn't apply any changes."
         logger.warn(message)
         parent
     }
  }

}