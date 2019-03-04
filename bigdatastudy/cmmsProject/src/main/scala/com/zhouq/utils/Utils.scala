package com.zhouq.utils

import java.text.SimpleDateFormat

/**
  * 工具类
  *
  */
object Utils {

  /**
    * 获取充值时间
    * @param startTime 充值开始时间
    * @param endTime 充值到账时间
    * @return
    */
  def caculateRqt(startTime: String, endTime: String): Long = {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val start: Long = dateFormat.parse(startTime.substring(0, 17)).getTime
    val end: Long = dateFormat.parse(endTime).getTime
    end - start
  }

}
