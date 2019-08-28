package com.utils

object CheckArgsUtil {
  def checkArgs(args:Array[String]):Boolean={
    if(args.length==1) true
    else false
  }
}
