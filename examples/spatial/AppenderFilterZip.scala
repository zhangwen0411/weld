import spatial._
import org.virtualized._

// FIXME(zhangwen): this doesn't work ("You done goofed").
object AppenderFilterZip extends SpatialApp {
  import IR._

  //w |x: vec[i32], y: vec[i32]| result(for(zip(x, y), appender[i32], |b, i, e| if (e.$0+e.$1 > 42, merge(b, e.$0*e.$1), b)))
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
      val tmp_3 = Reg[Index](0.to[Index])
      val round_blk = 16 * 4
      Sequential(tmp_0 by round_blk) { i =>
        val tmp_4 = FIFO[Int](16)
        val tmp_5 = FIFO[Int](16)
        val tmp_6 = FIFO[Int](16)
        val tmp_7 = FIFO[Int](16)
        Parallel {
          {  // #0
            val base = (i + 0*16).to[Index]
            val block_size = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val tmp_8 = SRAM[Int](16)
            val tmp_9 = SRAM[Int](16)
            Parallel {
              tmp_8 load x_0(base::base+block_size)
              tmp_9 load y_0(base::base+block_size)
            }  // Parallel


            Pipe(block_size by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val tmp_10 = tmp_8(ii)
              val tmp_11 = tmp_9(ii)


              Sequential {
                val tmp_12 = tmp_10 + tmp_11
                val tmp_13 = 42.to[Int]
                val tmp_14 = tmp_12 > tmp_13
                if (tmp_14) {
                  val tmp_15 = tmp_10 * tmp_11
                  tmp_4.enq(tmp_15)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #0

          {  // #1
            val base = (i + 1*16).to[Index]
            val block_size = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val tmp_16 = SRAM[Int](16)
            val tmp_17 = SRAM[Int](16)
            Parallel {
              tmp_16 load x_0(base::base+block_size)
              tmp_17 load y_0(base::base+block_size)
            }  // Parallel


            Pipe(block_size by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val tmp_18 = tmp_16(ii)
              val tmp_19 = tmp_17(ii)


              Sequential {
                val tmp_20 = tmp_18 + tmp_19
                val tmp_21 = 42.to[Int]
                val tmp_22 = tmp_20 > tmp_21
                if (tmp_22) {
                  val tmp_23 = tmp_18 * tmp_19
                  tmp_5.enq(tmp_23)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #1

          {  // #2
            val base = (i + 2*16).to[Index]
            val block_size = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val tmp_24 = SRAM[Int](16)
            val tmp_25 = SRAM[Int](16)
            Parallel {
              tmp_24 load x_0(base::base+block_size)
              tmp_25 load y_0(base::base+block_size)
            }  // Parallel


            Pipe(block_size by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val tmp_26 = tmp_24(ii)
              val tmp_27 = tmp_25(ii)


              Sequential {
                val tmp_28 = tmp_26 + tmp_27
                val tmp_29 = 42.to[Int]
                val tmp_30 = tmp_28 > tmp_29
                if (tmp_30) {
                  val tmp_31 = tmp_26 * tmp_27
                  tmp_6.enq(tmp_31)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #2

          {  // #3
            val base = (i + 3*16).to[Index]
            val block_size = min(max(tmp_0 - base, 0.to[Index]), 16.to[Index])
            val tmp_32 = SRAM[Int](16)
            val tmp_33 = SRAM[Int](16)
            Parallel {
              tmp_32 load x_0(base::base+block_size)
              tmp_33 load y_0(base::base+block_size)
            }  // Parallel


            Pipe(block_size by 1) { ii =>
              val i_0 = (base + ii).to[Long]
              val tmp_34 = tmp_32(ii)
              val tmp_35 = tmp_33(ii)


              Sequential {
                val tmp_36 = tmp_34 + tmp_35
                val tmp_37 = 42.to[Int]
                val tmp_38 = tmp_36 > tmp_37
                if (tmp_38) {
                  val tmp_39 = tmp_34 * tmp_35
                  tmp_7.enq(tmp_39)

                } else {

                }

              }  // Sequential
            }  // Pipe
          }  // #3

        }  // Parallel
        val tmp_40 = tmp_4.numel
        val tmp_41 = tmp_4.numel+tmp_5.numel
        val tmp_42 = tmp_4.numel+tmp_5.numel+tmp_6.numel
        val tmp_43 = tmp_4.numel+tmp_5.numel+tmp_6.numel+tmp_7.numel
        Parallel {
          tmp_2(tmp_3+tmp_40-tmp_4.numel::tmp_3+tmp_40) store tmp_4
          tmp_2(tmp_3+tmp_41-tmp_5.numel::tmp_3+tmp_41) store tmp_5
          tmp_2(tmp_3+tmp_42-tmp_6.numel::tmp_3+tmp_42) store tmp_6
          tmp_2(tmp_3+tmp_43-tmp_7.numel::tmp_3+tmp_43) store tmp_7
        }  // Parallel
        tmp_3 := tmp_3 + tmp_43
      }  // Sequential
      len := tmp_3
    }
    pack(getMem(tmp_2), getArg(len))
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
    val b = Array.tabulate(N){ i => (i % 113) }
    val (resultArr, resultLen) = unpack(spatialProg(a, b))

    val goldArr = a.zip(b){ (x, y) => pack(x, y) }.filter( p => p._1+p._2 > 42 ).map( p => p._1*p._2 )
    val cksum = (goldArr.length == resultLen) && (goldArr.zip(resultArr){_ == _}.reduce{_&&_})

    printArrayTruncated(resultArr, resultLen, "result:")
    printArray(goldArr, "gold:")

    println("PASS: " + cksum  + " (AppenderFilterZip)")
  }
}


