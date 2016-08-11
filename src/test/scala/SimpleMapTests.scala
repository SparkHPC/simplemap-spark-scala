import org.scalatest._

import breeze.linalg._
import edu.luc.cs.SimpleMap._

class SimpleMapTests extends FlatSpec with Matchers {

  "Breeze Basics" should "work as expected" in {
    val a = Array(1.0, 2.0, 3.0)
    val dv = DenseVector(a)
    dv should be(DenseVector(1.0, 2.0, 3.0))
  }

  "SimpleMap" should "generate wrapped matrices" in {
    val data = generate(1, 3)
    data.length should be(3)
    for (i <- 0 until data.length) {
      data(i).rows should be(MB_OF_FLOATS)
      data(i).cols should be(3)
    }
  }
}
