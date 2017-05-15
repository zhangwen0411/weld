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
      // The % operator doesn't work on registers; the `+0` is a hack
      // to make it work.
      assert((tmp_0+0) % 16 == 0)
      val tmp_3 = Reduce(Reg[Int])(tmp_0 by 16){ i =>
        // Tiling: bring BLK_SIZE elements into SRAM.
        val tmp_4 = SRAM[Int](16)
        tmp_4 load x_0(i::i+16)
        Reduce(Reg[Int])(16 by 1){ ii =>
          val i1_0 = (i + ii).to[Long]
          val e1_0 = tmp_4(ii)

          val b1_0 = 0.to[Int]
          // The % operator doesn't work on registers; the `+0` is a hack
          // to make it work.
          assert((tmp_1+0) % 16 == 0)
          val tmp_5 = Reduce(Reg[Int])(tmp_1 by 16){ i =>
            // Tiling: bring BLK_SIZE elements into SRAM.
            val tmp_6 = SRAM[Int](16)
            tmp_6 load y_0(i::i+16)
            Reduce(Reg[Int])(16 by 1){ ii =>
              val i2_0 = (i + ii).to[Long]
              val e2_0 = tmp_6(ii)

              val b2_0 = 0.to[Int]
              val tmp_7 = e1_0 * e2_0
              val tmp_8 = b2_0 + tmp_7

              tmp_8
            } { _+_ }  // Reduce
          } { _+_ } + b1_0  // Reduce

          tmp_5
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
