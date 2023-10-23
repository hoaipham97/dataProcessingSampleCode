package demo.pdsa

import com.clearspring.analytics.stream.frequency.CountMinSketch
import org.openjdk.jol.info.{ClassData, ClassLayout}

import java.util.UUID
import scala.collection.mutable
object CountMinSketch {

  val LENGTH = 100000
  def main(args: Array[String]): Unit = {
//    firstDemo()
    secondDemo()
  }

  def firstDemo(): Unit = {
    val countMinSketch = new CountMinSketch(0.001, 0.99, 1)
    countMinSketch.add("75.245.10.1", 1)
    countMinSketch.add("10.125.22.20", 1)
    countMinSketch.add("192.170.0.1", 2)

    System.out.println(countMinSketch.estimateCount("192.170.0.1"))
    System.out.println(countMinSketch.estimateCount("999.999.99.99"))
  }

  def secondDemo(): Unit = {
    val countMinSketch = new CountMinSketch(1.0 / LENGTH, 0.999, 1)
    val arrBuffer = mutable.ArrayBuffer[String]()

    for (_ <- 0 until  LENGTH) {
      val uuid = UUID.randomUUID().toString
      arrBuffer.append(uuid)
    }

    val arr = arrBuffer.toArray

    for (i: Int <- 0 until  LENGTH * 10) {
      countMinSketch.add(arr(i % LENGTH), 1)
    }

    println(countMinSketch.estimateCount(arr(1)))
    println(countMinSketch.estimateCount(UUID.randomUUID().toString))
//    println(ClassLayout.parseInstance(countMinSketch).toPrintable)
  }

}
