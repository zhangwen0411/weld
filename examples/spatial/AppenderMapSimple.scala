import spatial._
import org.virtualized._

object AppenderMapSimple extends SpatialApp {
  import IR._

  //w |x: vec[i32]| map(x, |num: i32| num * 2)
  @virtualize
  def spatialProg(param_x_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_x_0.length)
    val x_0 = DRAM[Int](tmp_0)
    setMem(x_0, param_x_0)
    val len = ArgOut[Index]
    val tmp_1 = DRAM[Int](tmp_0)
    Accel {
      Pipe(tmp_0 by 16) { i =>
        val tmp_2 = SRAM[Int](16)
        val block_size = min(tmp_0 - i, 16.to[Index])
        val tmp_4 = SRAM[Int](16)
        Parallel {
          tmp_4 load x_0(i::i+block_size)
        }  // Parallel

        Pipe(block_size by 1) { tmp_3 =>
          val i_0 = (i + tmp_3).to[Long]
          val x_1 = tmp_4(tmp_3)


          Sequential {
            val tmp_5 = 2.to[Int]
            val tmp_6 = x_1 * tmp_5
            tmp_2(tmp_3) = tmp_6

          }
        }
        tmp_1(i::i+block_size) store tmp_2
      }
      len := tmp_0
    }
    pack(getMem(tmp_1), getArg(len))
  }

  @virtualize
  def main() {
    val N = 2017
    val a = Array.tabulate(N){ i => (i % 97) }
    val (resultArr, resultLen) = unpack(spatialProg(a))

    val gold = a.map(x => x*2)
    val cksum = (resultLen == gold.length) && (resultArr.zip(gold){_ == _}.reduce{_&&_})

    printArray(resultArr, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (AppenderMapSimple)")
  }
}


