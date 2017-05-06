import spatial._
import org.virtualized._

object VecMergerNestedMerge extends SpatialApp {
  import IR._

  // |x:vec[i32]| result(for(x, vecmerger[i32,+](x), |b,i,e| merge(merge(b, {i,e*7}), {i,e*8})))
  @virtualize
  def spatialProg(param_x_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_x_0.length)
    val x_0 = DRAM[Int](tmp_0)
    setMem(x_0, param_x_0)
    val tmp_1 = DRAM[Int](tmp_0)
    Accel {
      Sequential {
        Pipe(tmp_0 by 16) { i =>
          val ss = SRAM[Int](16)
          ss load x_0(i::i+16)
          tmp_1(i::i+16) store ss
        } // Pipe
        assert((tmp_0+0) % 16 == 0)
        // Bring a block of destination DRAM into SRAM.
        Pipe(tmp_0 by 16) { tmp_3 =>
          val tmp_2 = SRAM[Int](16)
          tmp_2 load tmp_1(tmp_3::tmp_3+16)
          
          assert((tmp_0+0) % 4 == 0)
          val piece_len = tmp_0/4
          assert(piece_len % 16 == 0)
          MemFold(tmp_2)(tmp_0 by piece_len par 4) { i1 =>
            val tmp_5 = SRAM[Int](16)
            Foreach(16 by 1) { ii => tmp_5(ii) = 0 }
            
            Pipe(piece_len by 16) { i2 =>
              val base = i1 + i2
              val tmp_4 = SRAM[Int](16)
              tmp_4 load x_0(base::base+16)
              
              Pipe(16 by 1) { i3 =>
                // TODO(zhangwen): Spatial indices are 32-bit.
                val i_0 = (base + i3).to[Long]
                val e_0 = tmp_4(i3)
                
                Sequential {
                  val tmp_6 = 7.to[Int]
                  val tmp_7 = e_0 * tmp_6
                  val tmp_9 = i_0.to[Index] - tmp_3
                  if (tmp_9 >= 0 && tmp_9 < 16) {
                    tmp_5(tmp_9) = tmp_5(tmp_9) + tmp_7
                  }
                  val tmp_10 = 8.to[Int]
                  val tmp_11 = e_0 * tmp_10
                  val tmp_13 = i_0.to[Index] - tmp_3
                  if (tmp_13 >= 0 && tmp_13 < 16) {
                    tmp_5(tmp_13) = tmp_5(tmp_13) + tmp_11
                  }
                  
                }
              }  // Pipe
            }  // Foreach
            
            tmp_5
          } { _+_ }  // MemReduce
          
          tmp_1(tmp_3::tmp_3+16) store tmp_2
        }  // Pipe
      }
    }
    getMem(tmp_1)
  }

  @virtualize
  def main() {
    val N = 32*32
    val a = Array.tabulate(N){ i => (i % 97) }
    val result = spatialProg(a)

    val gold = Array.tabulate(N){ i => ((i % 97) * 16) }
    val cksum = result.zip(gold){_ == _}.reduce{_&&_}

    printArray(result, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (VecMergerNestedMerge)")
  }
}

