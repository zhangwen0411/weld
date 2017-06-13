import spatial._
import org.virtualized._

object MergerSimple extends SpatialApp {
  import IR._

  //w |v: vec[i64]| result(for(v, merger[i64,+], |b, i, e| merge(b, e*i)))
  @virtualize
  def spatialProg(param_v_0: Array[Long]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_v_0.length)
    val v_0 = DRAM[Long](tmp_0)
    setMem(v_0, param_v_0)
    val out = ArgOut[Long]
    Accel {
      val tmp_1 = 0.to[Long]
      val tmp_2 = Reduce(Reg[Long])(tmp_0 by 16){ i =>
        val block_len = min(tmp_0 - i, 16.to[Index])
        val tmp_3 = SRAM[Long](16)
        Parallel {
          tmp_3 load v_0(i::i+block_len)
        }  // Parallel
        Reduce(Reg[Long])(block_len by 1){ ii =>
          val i_0 = (i + ii).to[Long]
          val e_0 = tmp_3(ii)
          val b_0 = 0.to[Long]
          val tmp_4 = e_0 * i_0
          val tmp_5 = b_0 + tmp_4
          tmp_5
        } { _+_ }  // Reduce
      } { _+_ } + tmp_1  // Reduce
      out := tmp_2
    }
    getArg(out)
  }

  @virtualize
  def main() {
    val N = 2017
    val a = Array.tabulate(N){ i => (i % 97).to[Long] }

    val result = spatialProg(a)
    println("result: " + result)

    val gold = a.zip(Array.tabulate(N){ i => i.to[Long] }){_*_}.reduce{_+_}
    println("gold: " + gold)
    val cksum = gold == result
    println("PASS: " + cksum + " (MergerSimple)")
  }
}
