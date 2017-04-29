import spatial._
import org.virtualized._

object VecMergerSimple2 extends SpatialApp {
  import IR._

  // |x:vec[i32], y:vec[i64]| result(for(y, vecmerger[i32,+](x), |b,i,e| merge(b, {e,1})))
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
    val tmp_2 = DRAM[Int](tmp_0)
    Accel {
      Pipe(tmp_0 by 16) { i =>
        val ss = SRAM[Int](16)
        ss load x_0(i::i+16)
        tmp_2(i::i+16) store ss
      }
      assert((tmp_0+0) % 16 == 0)
      // Bring a block of destination DRAM into SRAM.
      Pipe(tmp_0 by 16) { tmp_4 =>
        val tmp_3 = SRAM[Int](16)
        tmp_3 load tmp_2(tmp_4::tmp_4+16)
        
        assert((tmp_1+0) % 4 == 0)
        val piece_len = tmp_1/4
        assert(piece_len % 16 == 0)
        MemFold(tmp_3)(tmp_1 by piece_len par 4) { i1 =>
          val tmp_6 = SRAM[Int](16)
          Foreach(16 by 1) { ii => tmp_6(ii) = 0 }
          
          Pipe(piece_len by 16) { i2 =>
            val base = i1 + i2
            val tmp_5 = SRAM[Long](16)
            tmp_5 load y_0(base::base+16)
            
            Pipe(16 by 1) { i3 =>
              // TODO(zhangwen): Spatial indices are 32-bit.
              val i_0 = (base + i3).to[Long]
              val e_0 = tmp_5(i3)
              
              val tmp_7 = 1.to[Int]
              val tmp_9 = e_0.to[Index] - tmp_4
              if (tmp_9 >= 0 && tmp_9 < 16) {
                tmp_6(tmp_9) = tmp_6(tmp_9) + tmp_7
              }
              
            }  // Pipe
          }  // Foreach
          
          tmp_6
        } { _+_ }  // MemReduce
        
        tmp_2(tmp_4::tmp_4+16) store tmp_3
      }  // Pipe
    }
    getMem(tmp_2)
  }

  @virtualize
  def main() {
    val N = 256
    val a = Array.tabulate(N){ i => (i % 97) }

    var M = 256*128
    val b = Array.tabulate(M){ i => (i % 256).to[Long] }

    val result = spatialProg(a, b)

    val gold = Array.tabulate(N){ i => (i % 97) + 128 }
    val cksum = result.zip(gold){_ == _}.reduce{_&&_}

    printArray(result, "result:")
    printArray(gold, "gold:")

    println("PASS: " + cksum  + " (VecMergerSimple2)")
  }
}

