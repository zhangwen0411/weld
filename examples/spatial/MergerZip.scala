import spatial._
import org.virtualized._

object MergerZip extends SpatialApp {
  import IR._

  //w |x: vec[i64], y: vec[i64]| result(for(zip(x, y), merger[i64, +], |b, i, e| merge(b, e.$0*e.$1)))
  @virtualize
  def spatialProg(param_x_0: Array[Long], param_y_0: Array[Long]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_x_0.length)
    val x_0 = DRAM[Long](tmp_0)
    setMem(x_0, param_x_0)
    val tmp_1 = ArgIn[Index]
    setArg(tmp_1, param_y_0.length)
    val y_0 = DRAM[Long](tmp_1)
    setMem(y_0, param_y_0)
    val out = ArgOut[Long]
    Accel {
      val tmp_2 = 0.to[Long]
      val tmp_4 = reduceTree(Seq[Index](tmp_0, tmp_1))(min)
      val tmp_3 = Reduce(Reg[Long])(tmp_4 by 16){ i =>
        val block_len = min(tmp_4 - i, 16.to[Index])
        val tmp_5 = SRAM[Long](16)
        val tmp_6 = SRAM[Long](16)
        Parallel {
          tmp_5 load x_0(i::i+block_len)
          tmp_6 load y_0(i::i+block_len)
        }  // Parallel
        Reduce(Reg[Long])(block_len by 1){ ii =>
          val i_0 = (i + ii).to[Long]
          val tmp_7 = tmp_5(ii)
          val tmp_8 = tmp_6(ii)
          val b_0 = 0.to[Long]
          val tmp_9 = tmp_7 * tmp_8
          val tmp_10 = b_0 + tmp_9
          tmp_10
        } { _+_ }  // Reduce
      } { _+_ } + tmp_2  // Reduce
      out := tmp_3
    }
    getArg(out)
  }

  @virtualize
  def main() {
    val N = 2017
    val a = Array.tabulate(N){ i => (i % 97).to[Long] }
    val b = Array.tabulate(N + 10){ i => (i % 101).to[Long] }

    val result = spatialProg(a, b)
    println("result: " + result)

    val gold = a.zip(b){_*_}.reduce{_+_}
    println("gold: " + gold)
    val cksum = gold == result
    println("PASS: " + cksum + " (MergerZip)")
  }
}
