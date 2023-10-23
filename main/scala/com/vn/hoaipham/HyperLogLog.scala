package demo.pdsa

import com.google.common.hash.{HashFunction, Hashing}
import net.agkn.hll.HLL
import org.apache.hadoop.util.bloom.Key

import java.nio.charset.Charset
import java.util.UUID

object HyperLogLog {

  val nElements = 100000000
  def main(args: Array[String]): Unit = {
    val hashFunction = Hashing.murmur3_128()
    val hll = new HLL(14, 5)
    for (_ <- 1 to nElements) {
      val uuid = UUID.randomUUID().toString
      val cs: CharSequence = uuid
      val hashedValue = hashFunction.newHasher().putString(cs, Charset.forName("UTF-8")).hash().asLong()
      hll.addRaw(hashedValue)
    }

    println(hll.cardinality())
  }

}
