package io.hydrosphere.mist.utils

import java.net.{Inet4Address, InetAddress, NetworkInterface}

import scala.collection.JavaConverters._

object NetUtils {

  /**
    * Copied from spark - see org.apache.spark.utils.Utils.findLocalInetAddress
    * windows related thing removed
    */
  def findLocalInetAddress(): InetAddress = {
    val address = InetAddress.getLocalHost
    if (address.isLoopbackAddress) {
      val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toList
      for (ni <- activeNetworkIFs) {
        val addresses = ni.getInetAddresses.asScala.toList
          .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress)
        if (addresses.nonEmpty) {
          val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
          val strippedAddress = InetAddress.getByAddress(addr.getAddress)
          return strippedAddress
        }
      }
    }
    address
  }
}
