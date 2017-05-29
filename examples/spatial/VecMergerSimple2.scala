import spatial._
import org.virtualized._

object VecMergerSimple2 extends SpatialApp {
  import IR._

  //w |x:vec[i32], y:vec[i64]| result(for(y, vecmerger[i32,+](x), |b,i,e| merge(b, {e,1})))
  @virtualize
  def spatialProg(param_x_0: Array[Int], param_y_0: Array[Long]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_x_0.length)
    val x_0 = DRAM[Int](tmp_0)
    setMem(x_0, param_x_0)
    val tmp_1 = ArgIn[Index]
    setArg(tmp_1, param_y_0.length)
    val y_0 = DRAM[Long](tmp_1)
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

          Pipe(tmp_1 by round_blk) { i =>
            val base = par_offset + i
            val data_sram = SRAM[Long](32)
            val data_block_size =
            min(max(tmp_1 - base, 0.to[Index]), 32.to[Index])
            data_sram load y_0(base::base+data_block_size)

            Pipe(data_block_size by 1) { ii =>
              // TODO(zhangwen): Spatial indices are 32-bit.
              val i_0 = (base + ii).to[Long]
              val e_0 = data_sram(ii)

              Sequential {
                val tmp_7 = 1.to[Int]
                val tmp_9 = e_0.to[Index] - tmp_4
                if (tmp_9 >= 0 && tmp_9 < tmp_5) {
                  tmp_6(tmp_9) = tmp_6(tmp_9) + tmp_7
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
    val N = 256
    val a = Array.tabulate(N){ i => (i % 97) }

    var M = 256*128
    val b = Array.tabulate(M){ i => (i % 256).to[Long] }

    val (resultArr, resultLen) = unpack(spatialProg(a, b))

    val gold = Array.tabulate(N){ i => (i % 97) + 128 }
    val cksum = (resultLen == gold.length) && (resultArr.zip(gold){_ == _}.reduce{_&&_})

    printArray(resultArr, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (VecMergerSimple2)")
  }
}

