import spatial._
import org.virtualized._

object AppenderFilterSimple extends SpatialApp {
  import IR._

  //w |v: vec[i32]| result(for(v, appender[i32], |b, i, e| if (e > 42, merge(b, e*e), b)))
  @virtualize
  def spatialProg(param_v_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_v_0.length)
    val v_0 = DRAM[Int](tmp_0)
    setMem(v_0, param_v_0)
    val len = ArgOut[Index]
    val tmp_1 = DRAM[Int](tmp_0)
    Accel {
      val tmp_2 = Reg[Index](0.to[Index])
      val round_blk = 16 * 4
      Sequential(tmp_0 by round_blk) { i =>
        val tmp_3 = FIFO[Int](16)
        val tmp_4 = FIFO[Int](16)
        val tmp_5 = FIFO[Int](16)
        val tmp_6 = FIFO[Int](16)
        Parallel {
          {  // #0
            val base = (i + 0*16).to[Index]
            val block_len = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val sram_data = SRAM[Int](16)
            sram_data load v_0(base::base+block_len)

            Pipe(block_len by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val e_0 = sram_data(ii)

              Sequential {
                val tmp_7 = 42.to[Int]
                val tmp_8 = e_0 > tmp_7
                if (tmp_8) {
                  val tmp_9 = e_0 * e_0
                  tmp_3.enq(tmp_9)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #0

          {  // #1
            val base = (i + 1*16).to[Index]
            val block_len = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val sram_data = SRAM[Int](16)
            sram_data load v_0(base::base+block_len)

            Pipe(block_len by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val e_0 = sram_data(ii)

              Sequential {
                val tmp_10 = 42.to[Int]
                val tmp_11 = e_0 > tmp_10
                if (tmp_11) {
                  val tmp_12 = e_0 * e_0
                  tmp_4.enq(tmp_12)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #1

          {  // #2
            val base = (i + 2*16).to[Index]
            val block_len = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val sram_data = SRAM[Int](16)
            sram_data load v_0(base::base+block_len)

            Pipe(block_len by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val e_0 = sram_data(ii)

              Sequential {
                val tmp_13 = 42.to[Int]
                val tmp_14 = e_0 > tmp_13
                if (tmp_14) {
                  val tmp_15 = e_0 * e_0
                  tmp_5.enq(tmp_15)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #2

          {  // #3
            val base = (i + 3*16).to[Index]
            val block_len = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val sram_data = SRAM[Int](16)
            sram_data load v_0(base::base+block_len)

            Pipe(block_len by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val e_0 = sram_data(ii)

              Sequential {
                val tmp_16 = 42.to[Int]
                val tmp_17 = e_0 > tmp_16
                if (tmp_17) {
                  val tmp_18 = e_0 * e_0
                  tmp_6.enq(tmp_18)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #3

        }  // Parallel
        val tmp_19 = tmp_3.numel
        val tmp_20 = tmp_3.numel+tmp_4.numel
        val tmp_21 = tmp_3.numel+tmp_4.numel+tmp_5.numel
        val tmp_22 = tmp_3.numel+tmp_4.numel+tmp_5.numel+tmp_6.numel
        Parallel {
          tmp_1(tmp_2+tmp_19-tmp_3.numel::tmp_2+tmp_19) store tmp_3
          tmp_1(tmp_2+tmp_20-tmp_4.numel::tmp_2+tmp_20) store tmp_4
          tmp_1(tmp_2+tmp_21-tmp_5.numel::tmp_2+tmp_21) store tmp_5
          tmp_1(tmp_2+tmp_22-tmp_6.numel::tmp_2+tmp_22) store tmp_6
        }  // Parallel
        tmp_2 := tmp_2 + tmp_22
      }  // Sequential
      len := tmp_2
    }
    pack(getMem(tmp_1), getArg(len))
  }

  @virtualize
  def printArrayTruncated[T:Meta](array: Array[T], len: Int, header: String = ""): Void = {
    println(header)
    (0 until len) foreach { i => print(array(i).toString + " ") }
    println("")
  }

  @virtualize
  def main() {
    val N = 2053
    val a = Array.tabulate(N){ i => (i % 97) }
    val (resultArr, resultLen) = unpack(spatialProg(a))

    val goldArr = a.filter(x => x > 42).map(x => x * x)
    val cksum = (goldArr.length == resultLen) && (goldArr.zip(resultArr){_ == _}.reduce{_&&_})

    printArrayTruncated(resultArr, resultLen, "result:")
    printArray(goldArr, "gold:")

    println("PASS: " + cksum  + " (AppenderFilterSimple)")
  }
}


