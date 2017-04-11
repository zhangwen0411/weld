# Weld

[![Build Status](https://travis-ci.org/weld-project/weld.svg?branch=master)](https://travis-ci.org/weld-project/weld)

Weld is a language and runtime for improving the performance of data-intensive applications. It optimizes across libraries and functions by expressing the core computations in libraries using a common intermediate representation, and optimizing across each framework.

Modern analytics applications combine multiple functions from different libraries and frameworks to build complex workflows. Even though individual functions can achieve high performance in isolation, the performance of the combined workflow is often an order of magnitude below hardware limits due to extensive data movement across the functions. Weld’s take on solving this problem is to lazily build up a computation for the entire workflow, and then optimizing and evaluating it only when a result is needed.

You can join the discussion on Weld on our [Google Group](https://groups.google.com/forum/#!forum/weld-users) or post on the Weld mailing list at [weld-group@lists.stanford.edu](mailto:weld-group@lists.stanford.edu).

## Contents

  * [Building](#building)
      - [MacOS LLVM Installation](#macos-llvm-installation)
      - [Ubuntu LLVM Installation](#ubuntu-llvm-installation)
      - [Building Weld](#building-weld)
  * [Documentation](#documentation)
  * [Grizzly (Pandas on Weld)](#grizzly)
  * [Running an Interactive REPL](#running-an-interactive-repl)
  * [Benchmarking](#benchmarking)
  * [Spatial](#spatial)

## Building

To build Weld, you need [Rust 1.13 or higher](http://rust-lang.org) and [LLVM](http://llvm.org) 3.8.

To install Rust, follow the steps [here](https://rustup.rs). You can verify that Rust was installed correctly on your system by typing `rustc` into your shell.

#### MacOS LLVM Installation

To install LLVM on macOS, first install [brew](https://brew.sh/). Then:

```bash
$ brew install llvm38
$ export PATH=$PATH:/usr/local/bin
```

Weld's dependencies require `llvm-config`, so you may need to create a symbolic link so the correct `llvm-config` is picked up:

```bash
$ ln -s /usr/local/bin/llvm-config-3.8 /usr/local/bin/llvm-config
```

To make sure this worked correctly, run `llvm-config --version`. You should see `3.8.x`.

#### Ubuntu LLVM Installation

To install LLVM on Ubuntu :

```bash
$ sudo apt install llvm-3.8
$ sudo apt install clang-3.8
```

Weld's dependencies require `llvm-config`, so you may need to create a symbolic link so the correct `llvm-config` is picked up:

```bash
$ ln -s /usr/bin/llvm-config-3.8 /usr/local/bin/llvm-config
```

To make sure this worked correctly, run `llvm-config --version`. You should see `3.8.x`.

#### Building Weld

With LLVM and Rust installed, you can build Weld. Clone this repository, set the `WELD_HOME` environment variable, and build using `cargo`:

```bash
$ git clone https://www.github.com/weld-project/weld
$ cd weld/
$ export WELD_HOME=`pwd`
$ cargo build --release
```

Weld builds two dynamically linked libraries (`.so` files on Linux and `.dylib` files on macOS): `libweld` and `libweldrt`. Both of these libraries are found using `WELD_HOME`. By default, the libraries are in `$WELD_HOME/target/release` and `$WELD_HOME/weld_rt/target/release`.

Finally, run the unit and integration tests:

```bash
$ cargo test
```

## Documentation

The `docs/` directory contains documentation for the different components of Weld.

* [language.md](https://github.com/weld-project/weld/blob/master/docs/language.md) describes the syntax of the Weld IR.
* [api.md](https://github.com/weld-project/weld/blob/master/docs/api.md) describes the low-level C API for interfacing with Weld.
* [python.md](https://github.com/weld-project/weld/blob/master/docs/python.md) gives an overview of the Python API.
* [tutorial.md](https://github.com/weld-project/weld/blob/master/docs/tutorial.md) contains a tutorial for how to build a small vector library using Weld.

## Grizzly

**Grizzly** is a subset of [Pandas](http://pandas.pydata.org/) integrated with Weld. Details on how to use Grizzly are in
[`python/grizzly`](https://github.com/weld-project/weld/tree/master/python/grizzly).
Some example workloads that make use of Grizzly are in [`examples/python/grizzly`](https://github.com/weld-project/weld/tree/master/examples/python/grizzly).

## Running an Interactive REPL

* `cargo test` runs unit and integration tests. A test name substring filter can be used to run a subset of the tests:
   
   ```
   cargo test <substring to match in test name>
   ```

* The `target/release/repl` program is a simple "shell" where one can type Weld programs and see
  the results of parsing, macro substitution and type inference.

Example `repl` session:
```
> let a = 5 + 2; a + a
Raw structure: [...]

After macro substitution:
let a=((5+2));(a+a)

After inlining applies:
let a=((5+2));(a+a)

After type inference:
let a:i32=((5+2));(a:i32+a:i32)

Expression type: i32

> map([1, 2], |x| x+1)
Raw structure: [...]

After macro substitution:
result(for([1,2],appender[?],|b,x|merge(b,(|x|(x+1))(x))))

After inlining applies:
result(for([1,2],appender[?],|b,x|merge(b,(x+1))))

After type inference:
result(for([1,2],appender[i32],|b:appender[i32],x:i32|merge(b:appender[i32],(x:i32+1))))

Expression type: vec[i32]
```

## Benchmarking

`cargo bench` runs benchmarks under the `benches/` directory. The results of the benchmarks are written to a file called `benches.csv`. To specify specific benchmarks to run:

```
$ cargo bench [benchmark-name]
```

If a benchmark name is not provided, all benchmarks are run.

## Spatial

This branch contains preliminary support for generating [Spatial](https://github.com/stanford-ppl/spatial-lang)
code from Weld.  To see it in action, run `repl --spatial` and enter Weld expressions as usual.
The Spatial code generator will spit out Spatial code if it understands the expression.

Example Spatial `repl` session:
```
>> |v: vec[i64]| result(for(v, merger[i64,+], |b, i, e| merge(b, e)))
[...]

Spatial code:
@virtualize
def spatialProg(param_v_0: Array[Int]) = {
  val tmp_0 = ArgIn[Int]
  setArg(tmp_0, param_v_0.length)
  val v_0 = DRAM[Int](tmp_0)
  setMem(v_0, param_v_0)
  val out = ArgOut[Int]
  Accel {
    val tmp_1 = Reg[Int]
    assert((tmp_0+0) % 16 == 0)
    Reduce(tmp_1)(tmp_0 by 16){ i =>
      val tmp_2 = SRAM[Int](16)
      tmp_2 load v_0(i::i+16)
      Reduce(Reg[Int])(16 by 1){ ii =>
        val i_0 = i + ii
        val e_0 = tmp_2(ii)
        e_0
      }{ _+_ }
    }{ _+_ }
    val tmp_3 = tmp_1.value
    out := tmp_3
  }
  getArg(out)
}
```

To run the Spatial code above, you need a `main` function to call `spatialProg` with an array,
along with other boilerplate.  Here is an example:
```scala
import spatial._
import org.virtualized._

object Foo extends SpatialApp {
  import IR._

  @virtualize
  def spatialProg(param_v_0: Array[Int]) = {
    // [Insert Spatial code from above]
  }

  @virtualize
  def main() {
    // Generate an array of 160 integers.
    val N = 160
    val a = Array.tabulate(N){ i => i % 256 }

    val result = spatialProg(a)
    println("result: " + result)
  }
}
```

The [Spatial tutorial](http://spatial-lang.readthedocs.io/en/latest/tutorial.html) has
detailed instructions on setting up Spatial and running Spatial code.

If the Spatial backend doesn't understand the expression entered in the REPL, it will
generate an error message.  See [spatial.rs](weld/spatial.rs) for all patterns understood by the
Spatial backend.

