import spatial._
import org.virtualized._

object MergerNestedFor extends SpatialApp {
  import IR._

  //w |x: vec[i32], y: vec[i32]| result(for(x, merger[i32, +], |b1, i1, e1| for(y, b1, |b2, i2, e2| merge(b2, e1*e2))))
  @virtualize
  def spatialProg(param_x_0: Array[Int], param_y_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_x_0.length)
    val x_0 = DRAM[Int](tmp_0)
    setMem(x_0, param_x_0)
    val tmp_1 = ArgIn[Index]
    setArg(tmp_1, param_y_0.length)
    val y_0 = DRAM[Int](tmp_1)
    setMem(y_0, param_y_0)
    val out = ArgOut[Int]
    Accel {
      val tmp_2 = 0.to[Int]
      val tmp_3 = Reduce(Reg[Int])(tmp_0 by 16){ i =>
        val block = SRAM[Int](16)
        val block_len = min(tmp_0 - i, 16.to[Index])
        block load x_0(i::i+block_len)
        Reduce(Reg[Int])(block_len by 1){ ii =>
          val i1_0 = (i + ii).to[Long]
          val e1_0 = block(ii)

          val b1_0 = 0.to[Int]
          val tmp_4 = Reduce(Reg[Int])(tmp_1 by 16){ i =>
            val block = SRAM[Int](16)
            val block_len = min(tmp_1 - i, 16.to[Index])
            block load y_0(i::i+block_len)
            Reduce(Reg[Int])(block_len by 1){ ii =>
              val i2_0 = (i + ii).to[Long]
              val e2_0 = block(ii)

              val b2_0 = 0.to[Int]
              val tmp_5 = e1_0 * e2_0
              val tmp_6 = b2_0 + tmp_5

              tmp_6
            } { _+_ }  // Reduce
          } { _+_ } + b1_0  // Reduce

          tmp_4
        } { _+_ }  // Reduce
      } { _+_ } + tmp_2  // Reduce
      out := tmp_3
    }
    getArg(out)
  }

  @virtualize
  def main() {
    val Nx = 160
    val x = Array.tabulate(Nx){ i => i % 97 }
    val Ny = 320
    val y = Array.tabulate(Ny){ i => i % 111 }

    val result = spatialProg(x, y)
    println("result: " + result)

    val gold = x.reduce{_+_} * y.reduce{_+_}
    println("gold: " + gold)
    val cksum = gold == result
    println("PASS: " + cksum + " (MergerNestedFor)")
  }
}
