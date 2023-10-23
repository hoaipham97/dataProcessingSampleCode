package demo.pdsa

import org.apache.hadoop.util.bloom.{BloomFilter, Key}
import org.apache.hadoop.util.hash.MurmurHash
import org.openjdk.jol.info.ClassLayout

import java.util.UUID
import scala.collection.mutable

object BloomFilter {
  val LENGTH = 1000000
  def main(args : Array[String]) {

    val set = mutable.Set[String]()
    var test_uuid = ""
    for (i <- 1 to LENGTH) {
      val uuid = UUID.randomUUID().toString
      set.add(uuid)
      if (i == LENGTH / 10) {
        test_uuid = uuid
      }
    }

    val filter = new BloomFilter(28000000, 3, 1) // Murmurhash
    set.foreach( x => filter.add(new Key(x.getBytes)))
//
//    for (_ <- 0 to 20) {
//      println(filter.membershipTest(new Key(UUID.randomUUID().toString.getBytes)))
//    }
    println(filter.membershipTest(new Key(test_uuid.getBytes)))
  }

}
