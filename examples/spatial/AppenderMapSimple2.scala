import spatial._
import org.virtualized._

object AppenderMapSimple2 extends SpatialApp {
  import IR._

  // |x: vec[i32]| map(x, |num| num*result(for(x, merger[i32, +], |b, i, e| merge(b, e+5))))
  @virtualize
  def spatialProg(param_x_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_x_0.length)
    val x_0 = DRAM[Int](tmp_0)
    setMem(x_0, param_x_0)
    val tmp_1 = DRAM[Int](tmp_0)
    Accel {
      Sequential {
        assert((tmp_0+0) % 16 == 0)
        Pipe(tmp_0 by 16) { i =>
          val sram_data = SRAM[Int](16)
          val tmp_2 = SRAM[Int](16)
          sram_data load x_0(i::i+16)
          Foreach(16 by 1) { tmp_3 =>
            val i_0 = (i + tmp_3).to[Long]
            val x_1 = sram_data(tmp_3)
            
            Sequential {
              val tmp_4 = 0.to[Int]
              // The % operator doesn't work on registers; the `+0` is a hack
              // to make it work.
              assert((tmp_0+0) % 16 == 0)
              val tmp_5 = Reduce(Reg[Int])(tmp_0 by 16){ i =>
                // Tiling: bring BLK_SIZE elements into SRAM.
                val tmp_6 = SRAM[Int](16)
                tmp_6 load x_0(i::i+16)
                Reduce(Reg[Int])(16 by 1){ ii =>
                  val i_1 = (i + ii).to[Long]
                  val e_0 = tmp_6(ii)
                  
                  val b_1 = 0.to[Int]
                  val tmp_7 = 5.to[Int]
                  val tmp_8 = e_0 + tmp_7
                  val tmp_9 = b_1 + tmp_8
                  
                  tmp_9
                } { _+_ }  // Reduce
              } { _+_ } + tmp_4  // Reduce
              val tmp_10 = x_1 * tmp_5
              tmp_2(tmp_3) = tmp_10
              
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

    val factor = a.reduce(_+_) + 5 * a.length
    val gold = a.map(x => x * factor)
    val cksum = result.zip(gold){_ == _}.reduce{_&&_}

    printArray(result, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (AppenderMapSimple2)")
  }
}


