package com.utils

object LogicUtils {
  // 此方法处理请求数
  def request(requestmode: Int, processnode: Int): List[Double] = {
    var (a, b, c) = (0.0, 0.0, 0.0)
    if (requestmode == 1 && processnode >= 1)
      a = 1.0
    if (requestmode == 1 && processnode >= 2)
      b = 1.0
    if (requestmode == 1 && processnode == 3)
      c = 1.0
    List(a, b, c)
  }

  // 此方法处理展示点击数

  def click(requestmode: Int, iseffective: Int): List[Double] = {
    var (a, b) = (0.0, 0.0)
    if (requestmode == 2 && iseffective == 1)
      a = 1.0
    if (requestmode == 3 && iseffective == 1)
      b = 1.0
    List(a, b)
  }

  // 此方法处理竞价操作

  def Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, WinPrice: Double, adpayment: Double): List[Double] = {
    var (a, b, c, d) = (0.0, 0.0, 0.0, 0.0)
    if (iseffective == 1 && isbilling == 1 && isbid == 1)
      a = 1.0
    if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0)
      b = 1.0
    if (iseffective == 1 && isbilling == 1 && iswin == 1)
      c = WinPrice / 1000
    if (iseffective == 1 && isbilling == 1 && iswin == 1)
      d = adpayment / 1000
    List(a, b, c, d)
  }

}
