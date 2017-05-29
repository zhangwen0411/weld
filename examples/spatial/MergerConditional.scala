import spatial._
import org.virtualized._

object MergerConditional extends SpatialApp {
  import IR._

  //w |v: vec[i32]| result(for(v, merger[i32,+], |b, i, e| if(e>0, merge(b, e), merge(b, -e))))
  @virtualize
  def spatialProg(param_v_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_v_0.length)
    val v_0 = DRAM[Int](tmp_0)
    setMem(v_0, param_v_0)
    val out = ArgOut[Int]
    Accel {
      val tmp_1 = 0.to[Int]
      val tmp_2 = Reduce(Reg[Int])(tmp_0 by 16){ i =>
        val block = SRAM[Int](16)
        val block_len = min(tmp_0 - i, 16.to[Index])
        block load v_0(i::i+block_len)
        Reduce(Reg[Int])(block_len by 1){ ii =>
          val i_0 = (i + ii).to[Long]
          val e_0 = block(ii)

          val b_0 = 0.to[Int]
          val tmp_3 = 0.to[Int]
          val tmp_4 = e_0 > tmp_3
          val tmp_5 = b_0 + e_0
          val tmp_6 = -e_0
          val tmp_7 = b_0 + tmp_6
          val tmp_8 = mux(tmp_4, tmp_5, tmp_7)

          tmp_8
        } { _+_ }  // Reduce
      } { _+_ } + tmp_1  // Reduce
      out := tmp_2
    }
    getArg(out)
  }

  @virtualize
  def main() {
    val N = 1600
    val a = Array.tabulate(N){ i => if (i % 2 == 0) i % 97 else -(i % 97) }

    val result = spatialProg(a)
    println("result: " + result)

    val gold = a.map(i => abs(i)).reduce{_+_}
    println("gold: " + gold)
    val cksum = gold == result
    println("PASS: " + cksum + " (MergerConditional)")
  }
}
