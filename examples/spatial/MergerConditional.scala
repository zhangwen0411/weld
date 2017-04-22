import spatial._
import org.virtualized._

object MergerConditional extends SpatialApp {
  import IR._

  // |v: vec[i32]| result(for(v, merger[i32,+], |b, i, e| if(e>0, merge(b, e), merge(b, -e))))
  @virtualize
  def spatialProg(param_v_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_v_0.length)
    val v_0 = DRAM[Int](tmp_0)
    setMem(v_0, param_v_0)
    val out = ArgOut[Int]
    Accel {
      val tmp_1 : Int = 0
      assert((tmp_0+0) % 16 == 0)
      val tmp_2 = Reduce(Reg[Int])(tmp_0 by 16){ i =>
        val tmp_3 = SRAM[Int](16)
        tmp_3 load v_0(i::i+16)
        Reduce(Reg[Int])(16 by 1){ ii =>
          val i_0 = i + ii
          val e_0 = tmp_3(ii)
          val b_0 : Int = 0
          val tmp_4 = 0.to[Int]
          val tmp_5 = e_0 > tmp_4
          val tmp_6 = b_0 + e_0
          val tmp_7 = -e_0
          val tmp_8 = b_0 + tmp_7
          val tmp_9 = mux(tmp_5, tmp_6, tmp_8)
          tmp_9
        }{ _+_ }
      }{ _+_ }
      val tmp_10 = tmp_1 + tmp_2
      out := tmp_10
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
