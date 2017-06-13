import spatial._
import org.virtualized._

object VecMergerZip extends SpatialApp {
  import IR._

  //w |x:vec[i32], y:vec[i32]| result(for(zip(x, y), vecmerger[i32,+](x), |b,i,e| merge(b, {i,e.$0*e.$1})))
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
        val ss = SRAM[Int](16)
        ss load x_0(i::i+16)
        tmp_2(i::i+16) store ss
      } // Pipe
      Pipe(tmp_0 by 16) { tmp_4 =>
        val tmp_3 = SRAM[Int](16)
        val tmp_5 = min(tmp_0 - tmp_4, 16.to[Index])
        tmp_3 load tmp_2(tmp_4::tmp_4+tmp_5)

        val round_blk = 32 * 4
        MemFold(tmp_3)(4 by 1 par 4) { par_id =>
          val par_offset = par_id * 32
          val tmp_6 = SRAM[Int](16)
          Pipe(16 by 1) { ii => tmp_6(ii) = 0 }

          Pipe(tmp_0 by round_blk) { i =>
            val base = par_offset + i
            val data_block_size =
            min(max(tmp_0 - base, 0.to[Index]), 32.to[Index])
            val tmp_7 = SRAM[Int](32)
            val tmp_8 = SRAM[Int](32)
            Parallel {
              tmp_7 load x_0(base::base+data_block_size)
              tmp_8 load y_0(base::base+data_block_size)
            }  // Parallel


            Pipe(data_block_size by 1) { ii =>
              // TODO(zhangwen): Spatial indices are 32-bit.
              val i_0 = (base + ii).to[Long]
              val tmp_9 = tmp_7(ii)
              val tmp_10 = tmp_8(ii)


              Sequential {
                val tmp_11 = tmp_9 * tmp_10
                val tmp_13 = i_0.to[Index] - tmp_4
                if (tmp_13 >= 0 && tmp_13 < tmp_5) {
                  tmp_6(tmp_13) = tmp_6(tmp_13) + tmp_11
                }

              }  // Sequential
            }  // Pipe
          }  // Pipe

          tmp_6
        } { _+_ }  // MemFold

        tmp_2(tmp_4::tmp_4+tmp_5) store tmp_3
      }  // Pipe
      len := tmp_0
    }
    pack(getMem(tmp_2), getArg(len))
  }

  @virtualize
  def main() {
    val N = 2017
    val a = Array.tabulate(N){ i => (i % 97) }
    val b = Array.tabulate(N){ i => (i % 101) }
    val (resultArr, resultLen) = unpack(spatialProg(a, b))

    val gold = Array.tabulate(N){ i => (i % 97) * (i % 101 + 1) }
    val cksum = (resultLen == gold.length) && (resultArr.zip(gold){_ == _}.reduce{_&&_})

    printArray(resultArr, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (VecMergerZip)")
  }
}

