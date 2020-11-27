package com.atguigu.project

import java.util.Random

import scala.collection.mutable.ListBuffer


/**
 * @author jack
 *         Date: 2020-11-25
 *         Time: 14:17
 */
case class RanOpt[T](value:T,weight:Int)
object RandomOptions {
  def apply[T](opts:RanOpt[T]*): RandomOptions[T] = {
    val randomOptions: RandomOptions[T] = new RandomOptions[T]()
    for (opt<-opts){
      randomOptions.totalWeight += opt.weight
      for (i <- 1 to opt.weight){
        randomOptions.optsBffer += opt.value
      }
    }
    randomOptions
  }
}

class RandomOptions[T](opts:RanOpt[T]*){
  var optsBffer = new ListBuffer[T]
  var totalWeight = 0
  def getRandomOpt:T={
    val randomNum: Int = new Random().nextInt(totalWeight)
    optsBffer(randomNum)
  }
}
