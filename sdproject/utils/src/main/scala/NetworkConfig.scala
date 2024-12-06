package com.orange.network

import java.net.InetAddress

object NetworkConfig {
  val ip: String = InetAddress.getLocalHost.getHostAddress
  val port: Int = 50052
} 
