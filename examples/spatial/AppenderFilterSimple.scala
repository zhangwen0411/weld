import spatial._
import org.virtualized._

object AppenderFilterSimple extends SpatialApp {
  import IR._

  //w |v: vec[i32]| result(for(v, appender[i32], |b, i, e| if (e > 42, merge(b, e), b)))
  @virtualize
  def spatialProg(param_v_0: Array[Int]) = {
    val tmp_0 = ArgIn[Index]
    setArg(tmp_0, param_v_0.length)
    val v_0 = DRAM[Int](tmp_0)
    setMem(v_0, param_v_0)
    val len = ArgOut[Index]
    val tmp_1 = DRAM[Int](tmp_0)
    val tmp_3 = DRAM[Index](tmp_0)
    val tmp_4 = DRAM[Int](tmp_0)
    Accel {
      val tmp_2 = Reg[Index]
      // Compute condition on each element.
      assert((tmp_0+0) % 16 == 0)
      Pipe(tmp_0 by 16 par 4) { i =>
        val sram_data = SRAM[Int](16)
        val tmp_5 = SRAM[Index](16)
        val tmp_6 = SRAM[Int](16)
        sram_data load v_0(i::i+16)

        Pipe(16 by 1) { tmp_7 =>
          val i_0 = (i + tmp_7).to[Long]
          val e_0 = sram_data(tmp_7)
          Sequential {
            tmp_5(tmp_7) = 0.to[Index]
            val tmp_8 = 42.to[Int]
            val tmp_9 = e_0 > tmp_8
            if (tmp_9) {
              tmp_5(tmp_7) = 1.to[Index]
              tmp_6(tmp_7) = e_0

            } else {

            }

          }
        }

        Parallel {
          tmp_3(i::i+16) store tmp_5
          tmp_4(i::i+16) store tmp_6
        }
      }

      // Compute prefix sums sequentially.
      // FIXME(zhangwen): is it correct to use Pipe instead of Sequential?
      // TODO(zhangwen): this is too sequential.
      val prev = Reg[Index](0.to[Index])
      Pipe(tmp_0 by 16) { i =>
        val sram_acc = SRAM[Index](16)
        sram_acc load tmp_3(i::i+16)

        Pipe(16 by 1) { ii =>
          if (sram_acc(ii) != 0.to[Index]) {
            prev := prev + 1.to[Index]
            sram_acc(ii) = prev
          }
        }

        tmp_3(i::i+16) store sram_acc
      }

      // Write result to destination DRAM.
      tmp_2 := Reduce(Reg[Index])(tmp_0 by 16 par 4) { i =>
        val temp = FIFO[Int](16)
        val sram_acc = SRAM[Index](16)
        val sram_merge = SRAM[Int](16)

        Parallel {
          sram_acc load tmp_3(i::i+16)
          sram_merge load tmp_4(i::i+16)
        }

        val maxIndex = Reg[Index] // One past the actual max index.
        maxIndex := 0.to[Index]
        Pipe(16 by 1) { ii =>
          val index = sram_acc(ii)
          if (index != 0) {
            temp.enq(sram_merge(ii))
            maxIndex := index
          }
        }

        val numElems = temp.numel
        tmp_1(maxIndex-numElems::maxIndex) store temp
        numElems
      } { _+_ }

      len := tmp_2
    }
    pack(getMem(tmp_1), getArg(len))
  }

  @virtualize
  def printArrayTruncated[T:Meta](array: Array[T], len: Int, header: String = ""): Void = {
    println(header)
    (0 until len) foreach { i => print(array(i).toString + " ") }
    println("")
  }

  @virtualize
  def main() {
    val N = 32*32
    val a = Array.tabulate(N){ i => (i % 97) }
    val (resultArr, resultLen) = unpack(spatialProg(a))

    val goldArr = a.filter(x => x > 42)
    val cksum = (goldArr.length == resultLen) && (goldArr.zip(resultArr){_ == _}.reduce{_&&_})

    printArrayTruncated(resultArr, resultLen, "result:")
    printArray(goldArr, "gold:")

    println("PASS: " + cksum  + " (AppenderFilterSimple)")
  }
}


