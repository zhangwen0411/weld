import spatial._
import org.virtualized._

object VecMergerNestedMerge extends SpatialApp {
  import IR._

  //w |x:vec[i32]| result(for(x, vecmerger[i32,+](x), |b,i,e| merge(merge(b, {i,e*7}), {i,e*8})))
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
        val ss = SRAM[Int](16)
        ss load x_0(i::i+16)
        tmp_1(i::i+16) store ss
      } // Pipe
      Pipe(tmp_0 by 16) { tmp_3 =>
        val tmp_2 = SRAM[Int](16)
        val tmp_4 = min(tmp_0 - tmp_3, 16.to[Index])
        tmp_2 load tmp_1(tmp_3::tmp_3+tmp_4)

        val round_blk = 32 * 4
        MemFold(tmp_2)(4 by 1 par 4) { par_id =>
          val par_offset = par_id * 32
          val tmp_5 = SRAM[Int](16)
          Pipe(16 by 1) { ii => tmp_5(ii) = 0 }

          Pipe(tmp_0 by round_blk) { i =>
            val base = par_offset + i
            val data_block_size =
            min(max(tmp_0 - base, 0.to[Index]), 32.to[Index])
            val tmp_6 = SRAM[Int](32)
            Parallel {
              tmp_6 load x_0(base::base+data_block_size)
            }  // Parallel


            Pipe(data_block_size by 1) { ii =>
              // TODO(zhangwen): Spatial indices are 32-bit.
              val i_0 = (base + ii).to[Long]
              val e_0 = tmp_6(ii)


              Sequential {
                val tmp_7 = 7.to[Int]
                val tmp_8 = e_0 * tmp_7
                val tmp_10 = i_0.to[Index] - tmp_3
                if (tmp_10 >= 0 && tmp_10 < tmp_4) {
                  tmp_5(tmp_10) = tmp_5(tmp_10) + tmp_8
                }
                val tmp_11 = 8.to[Int]
                val tmp_12 = e_0 * tmp_11
                val tmp_14 = i_0.to[Index] - tmp_3
                if (tmp_14 >= 0 && tmp_14 < tmp_4) {
                  tmp_5(tmp_14) = tmp_5(tmp_14) + tmp_12
                }

              }  // Sequential
            }  // Pipe
          }  // Pipe

          tmp_5
        } { _+_ }  // MemFold

        tmp_1(tmp_3::tmp_3+tmp_4) store tmp_2
      }  // Pipe
      len := tmp_0
    }
    pack(getMem(tmp_1), getArg(len))
  }

  @virtualize
  def main() {
    val N = 32*32
    val a = Array.tabulate(N){ i => (i % 97) }
    val (resultArr, resultLen) = unpack(spatialProg(a))

    val gold = Array.tabulate(N){ i => ((i % 97) * 16) }
    val cksum = (resultLen == gold.length) && (resultArr.zip(gold){_ == _}.reduce{_&&_})

    printArray(resultArr, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (VecMergerNestedMerge)")
  }
}

