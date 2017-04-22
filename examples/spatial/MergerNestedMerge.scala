import spatial._
import org.virtualized._

object MergerNestedMerge extends SpatialApp {
  import IR._

  // |v: vec[i64]| result(for(v, merger[i64,+], |b, i, e| merge(merge(b, i), 5l*e)))
  @virtualize
  def spatialProg(param_v_0: Array[Long]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_v_0.length)
    val v_0 = DRAM[Long](tmp_0)
    setMem(v_0, param_v_0)
    val out = ArgOut[Long]
    Accel {
      val tmp_1 : Long = 0
      assert((tmp_0+0) % 16 == 0)
      val tmp_2 = Reduce(Reg[Long])(tmp_0 by 16){ i =>
        val tmp_3 = SRAM[Long](16)
        tmp_3 load v_0(i::i+16)
        Reduce(Reg[Long])(16 by 1){ ii =>
          val i_0 = (i + ii).to[Long]
          val e_0 = tmp_3(ii)
          val b_0 : Long = 0
          val tmp_4 = b_0 + i_0
          val tmp_5 = 5.to[Long]
          val tmp_6 = tmp_5 * e_0
          val tmp_7 = tmp_4 + tmp_6
          tmp_7
        }{ _+_ }
      }{ _+_ }
      val tmp_8 = tmp_1 + tmp_2
      out := tmp_8
    }
    getArg(out)
  }

  @virtualize
  def main() {
    val N = 1600
    val a = Array.tabulate(N){ i => (i % 97).to[Long] }

    val result = spatialProg(a)
    println("result: " + result)

    val gold = a.zip(Array.tabulate(N){ i => i.to[Long] }){(x, y) => (5*x+y)}.reduce{_+_}
    println("gold: " + gold)
    val cksum = gold == result
    println("PASS: " + cksum + " (MergerNestedMerge)")
  }
}
