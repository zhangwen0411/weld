import spatial._
import org.virtualized._

object AppenderMapSimple extends SpatialApp {
  import IR._

  // |x: vec[i32]| map(x, |num: i32| num * 2)
  @virtualize
  def spatialProg(param_x_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_x_0.length)
    val x_0 = DRAM[Int](tmp_0)
    setMem(x_0, param_x_0)
    val tmp_1 = DRAM[Int](tmp_0)
    Accel {
      Sequential {
        assert(tmp_0.value % 16 == 0)
        Pipe(tmp_0 by 16) { i =>
          val sram_data = SRAM[Int](16)
          val tmp_2 = SRAM[Int](16)
          sram_data load x_0(i::i+16)
          Foreach(16 by 1) { tmp_3 =>
            val i_0 = (i + tmp_3).to[Long]
            val x_1 = sram_data(tmp_3)
            
            Sequential {
              val tmp_4 = 2.to[Int]
              val tmp_5 = x_1 * tmp_4
              tmp_2(tmp_3) = tmp_5
              
            }
          }
          tmp_1(i::i+16) store tmp_2
        }
      }
    }
    getMem(tmp_1)
  }

  @virtualize
  def main() {
    val N = 32*32
    val a = Array.tabulate(N){ i => (i % 97) }
    val result = spatialProg(a)

    val gold = a.map(x => x*2)
    val cksum = result.zip(gold){_ == _}.reduce{_&&_}

    printArray(result, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (AppenderMapSimple)")
  }
}


