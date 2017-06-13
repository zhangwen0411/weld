import spatial._
import org.virtualized._

object AppenderMapSimple2 extends SpatialApp {
  import IR._

  //w |x: vec[i32]| map(x, |num| num*result(for(x, merger[i32, +], |b, i, e| merge(b, e+5))))
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
            val tmp_5 = 0.to[Int]
            val tmp_6 = Reduce(Reg[Int])(tmp_0 by 16){ i =>
              val block_len = min(tmp_0 - i, 16.to[Index])
              val tmp_7 = SRAM[Int](16)
              Parallel {
                tmp_7 load x_0(i::i+block_len)
              }  // Parallel
              Reduce(Reg[Int])(block_len by 1){ ii =>
                val i_1 = (i + ii).to[Long]
                val e_0 = tmp_7(ii)
                val b_1 = 0.to[Int]
                val tmp_8 = 5.to[Int]
                val tmp_9 = e_0 + tmp_8
                val tmp_10 = b_1 + tmp_9
                tmp_10
              } { _+_ }  // Reduce
            } { _+_ } + tmp_5  // Reduce
            val tmp_11 = x_1 * tmp_6
            tmp_2(tmp_3) = tmp_11

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
    val N = 1011
    val a = Array.tabulate(N){ i => (i % 97) }
    val (resultArr, resultLen) = unpack(spatialProg(a))

    val factor = a.reduce(_+_) + 5 * a.length
    val gold = a.map(x => x * factor)
    val cksum = (resultLen == gold.length) && (resultArr.zip(gold){_ == _}.reduce{_&&_})

    printArray(resultArr, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (AppenderMapSimple2)")
  }
}


