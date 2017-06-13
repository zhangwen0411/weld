import spatial._
import org.virtualized._

object AppenderMapZip extends SpatialApp {
  import IR._

  //w |x: vec[i32], y: vec[i32]| map(zip(x, y), |p| p.$0 * p.$1)
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
    val len = ArgOut[Index]
    val tmp_2 = DRAM[Int](tmp_0)
    Accel {
      Pipe(tmp_0 by 16) { i =>
        val tmp_3 = SRAM[Int](16)
        val block_size = min(tmp_0 - i, 16.to[Index])
        val tmp_5 = SRAM[Int](16)
        val tmp_6 = SRAM[Int](16)
        Parallel {
          tmp_5 load x_0(i::i+block_size)
          tmp_6 load y_0(i::i+block_size)
        }  // Parallel

        Pipe(block_size by 1) { tmp_4 =>
          val i_0 = (i + tmp_4).to[Long]
          val tmp_7 = tmp_5(tmp_4)
          val tmp_8 = tmp_6(tmp_4)


          Sequential {
            val tmp_9 = tmp_7 * tmp_8
            tmp_3(tmp_4) = tmp_9

          }
        }
        tmp_2(i::i+block_size) store tmp_3
      }
      len := tmp_0
    }
    pack(getMem(tmp_2), getArg(len))
  }

  @virtualize
  def main() {
    val N = 2017
    val a = Array.tabulate(N){ i => (i % 97) }
    val b = Array.tabulate(N){ i => (i % 113) }
    val (resultArr, resultLen) = unpack(spatialProg(a, b))

    val gold = a.zip(b) { _*_ }
    val cksum = (resultLen == gold.length) && (resultArr.zip(gold){_ == _}.reduce{_&&_})

    printArray(resultArr, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (AppenderMapZip)")
  }
}


