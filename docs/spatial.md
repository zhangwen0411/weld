# Spatial Backend Notes

## Overview

This document describes how the Spatial backend translates Weld code into
[Spatial](https://github.com/stanford-ppl/spatial-lang).

The translation to Spatial occurs at the end of the compilation process.  After the Weld compiler
generates a type-checked and optimized AST, the Spatial backend turns the AST into Spatial code in
textual form.  This means that Spatial code generation can take advantage of existing optimizations
applicable to Weld code.

Compilation to Spatial diverges from the ordinary Weld compilation code path roughly after the
transformation phase; the Spatial backend does not exploit the sequential IR (SIR) or LLVM code
generation components.

The generated Spatial code can then be synthesized and loaded onto an FPGA (or other supported
architectures), which will (hopefully) carry out the same Weld computation with different
performance characteristics.

## Getting Started

To see Spatial code generation in action, run `repl --spatial` and enter a Weld expression as
usual.  The Spatial code generator will spit out Spatial code if the expression is supported.

For example, for this Weld lambda:
```
|v: vec[i64]| result(for(v, merger[i64, +], |b, i, e| merge(b, e*i)))
```
the Spatial backend might generate the following code:
```scala
@virtualize
def spatialProg(param_v_0: Array[Long]) = {
  val tmp_0 = ArgIn[Index]
  setArg(tmp_0, param_v_0.length)
  val v_0 = DRAM[Long](tmp_0)
  setMem(v_0, param_v_0)
  val out = ArgOut[Long]
  Accel {
    val tmp_1 = 0.to[Long]
    assert((tmp_0+0) % 16 == 0)
    val tmp_2 = Reduce(Reg[Long])(tmp_0 by 16){ i =>
      // Tiling: bring BLK_SIZE elements into SRAM.
      val tmp_3 = SRAM[Long](16)
      tmp_3 load v_0(i::i+16)
      Reduce(Reg[Long])(16 by 1){ ii =>
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
```

To run the Spatial code above, you need a `main` function to call `spatialProg` with an array,
along with some boilerplate, like this:
```scala
import spatial._
import org.virtualized._

object MergerSimple extends SpatialApp {
  import IR._

  @virtualize
  def spatialProg(param_v_0: Array[Long]) = {
    // [Insert Spatial code from above]
  }

  @virtualize
  def main() {
    val N = 1600
    val a = Array.tabulate(N){ i => (i % 97).to[Long] }

    val result = spatialProg(a)
    println("result: " + result)

    val gold = a.zip(Array.tabulate(N){ i => i.to[Long] }){_*_}.reduce{_+_}
    println("gold: " + gold)
    val cksum = gold == result
    println("PASS: " + cksum + " (MergerSimple)")
  }
}
```

The [Spatial tutorial](http://spatial-lang.readthedocs.io/en/latest/tutorial.html) has
detailed instructions on setting up the Spatial environment and running Spatial code.

The REPL will print an error message if the Spatial backend doesn't support the expression entered.
The supported patterns are discussed below.

## Examples

Examples of Spatial code generation can be found at [examples/spatial](examples/spatial).

In each example source file, right above the definition of the generated `spatialProg` Scala
function is a single-line comment starting with `//w` followed by a Weld lambda, which is the Weld
source for the generated Spatial code.

To re-generate the examples, run `update_weld_tests.py`, which builds the Weld compiler with
`cargo build`, goes through each example source file, invokes the Weld REPL (in Spatial mode) on
the Weld code following `//w`, and sticks the generated Spatial code beneath the comment, replacing
any existing Scala function there.  Use the `--no-build` command line flag if you don't want to
build Weld before updating the tests.

## Supported Patterns

Although the Spatial backend cannot translate arbitrary Weld code, it does support many
common Weld features and usage patterns, which are discussed in this section.

### Data Types

Here is an overview of the supported Weld data types (limitations discussed in subsequent
sections):

* Weld **scalar types** are easily supported since they have equivalents in Spatial.
* Only Weld **vectors of primitive types** (e.g., `vec[i32]`) are supported.  The Spatial backend
  does not recognize, say, vectors of vectors (e.g., `vec[vec[i32]]`) or vectors of structs (e.g.,
  `vec[{i32, i32}]`).
* Weld **structs** are generally supported, although only structs of primitives (e.g., `{i32,
  i32}`) have been tested.
* Weld **builders**: common uses of `merger` and `vecmerger`, and limited use cases of `appender`,
  are supported (see below).

### Vectors

Weld `vec`s are simply implemented as Spatial DRAMs.  Each vector has two properties: `len`, the
vector length, and `len_bound`, a static bound on `len` used to declare the underlying Spatial
DRAM.  Currently, the only case where `len < len_bound` is when a "filter" is performed using an
`appender`.  A Weld lambda that returns a vector is translated to a Spatial function that returns a
pair `(dram, len)`.

### Merger

The Spatial implementation of a `merger` closely resembles its linear semantics in Weld: a merger
is simply represented as a primitive value in Spatial, and a `merge` yields a new merger.  For
example, a new merger defined by `merger[i32, +]` can corespond to `val tmp_0 = 0`, and `merger(b,
5)` corresponds to `val tmp_1 = b + 5`.  A `for` loop on a `merger` is simply translated to nested
`Reduce` blocks in Spatial.

### VecMerger

The Spatial backend only supports a `merge` into a `vecmerger` if it's inside a `for` loop.  Such a
`for` loop is implemented by making multiple passes through the "destination" vector of the
`vecmerger`: a block of the vector is loaded into an SRAM and the loop body is evaluated where only
`merge`s into indices included in the block take effect.

### Appender

`appender`s are much harder to support in general because the length of the resulting vector can be
unbounded, while the size of a Spatial DRAM needs to be declared statically.  Therefore, the
Spatial backend only supports `appender`s in two `for` loop use patterns: "map", where exactly one
`merge` occurs inside the loop body; and "filter", where at most one `merge` occurs inside the loop
body.  Note that in the "filter" case, the merged value doesn't have to match the one in the input
vector.

The implementation for "map" is trivial: we create a new vector of the same length as the input and
emit a parallel `Pipe` in Spatial to write to different locations of the resulting vector.

The current implementation for "filter" is as follows:

1. Evaluate the loop body over a chunk of the input, in parallel.  When a `merge` is evaluated, the
   value is pushed into a Spatial FIFO local to the parallel "thread" (e.g., if the loop body is
   evaluated `par 4` then there'll be four FIFOs).
2. Compute the starting index of each FIFO in the resulting vector based on the number of elements
   processed so far and a prefix sum of the FIFO sizes.  This step is sequential.
3. Once we know where each FIFO's content goes, store the FIFOs into the result DRAM in parallel.

The Spatial loops are unrolled for parallelism because this design is otherwise hard to implement
in Spatial.  Ideally we would declare a FIFO inside the loop and let Spatial duplicate it for
parallelism; however, if we declare a FIFO in the loop from Step 1, it's hard to refer to the FIFOs
in Steps 2 and 3 after the loop ends.

Loop unrolling makes it impossible to exploit design space exploration to figure out the optimal
amount of parallelism; ideally, parallelism should still be handled by Spatial. 

### Conditionals

The translation of Weld conditionals (`if` expressions) is split into two cases:

1. If the return value is a primitive type (e.g., `i32` or `merger[i32, +]`), we assume that no
   side effects take place in either "branch" of the conditional.  For example, even if a branch
   contains a merge into a `vecmerger`, the resulting `vecmerger` is lost because it's not returned
   by the conditional.  Thus, we sequentially emit code for both branches of the `if` and then emit
   a Spatial `mux` over the values of the two branches as the result of the `if`.
2. Otherwise, the backend only supports the case where both branches evaluate to mergers derived
   from the same merger (e.g., `if(cond, merge(b, 0), merge(b, 1))` for `b: appender[i32]`; here,
   both branches are derived from `b`).  A `mux` is not needed because the conditional's value is,
   in either case, the Spatial DRAM corresponding to `b`.  We therefore turn the conditional
   directly into a Weld `if`, which ensures that the correct side effects take place on `b`
   depending on the condition.

One current limitation is that a Weld conditional cannot return a different struct on each branch;
this is because structs aren't treated as "primitive" types.  This use case, for structs of
primitive types (or structs of structs of primitive types, etc.), can be supported by emitting a
`mux` for each primitive value contained in the struct.

### Let Expressions

Weld's `let` expression allows binding the result of an expression to a name.  Its syntax is `let
name = expr; body`.

To support `let` expressions, the Spatial backend first generates code for `expr` (call the result
symbol `s`), then remembers that `name` is an *alias* of `s` when generating code for `body` so
that whenever the identifier `name` comes up, it gets replaced with `s`.

Alias resolution is recursive: if `s` is itself an alias of another symbol, that symbol is used
instead.

This approach allows the compiler to, e.g., keep track of which builder a `merge` refers to when it
uses an alias; this helps figure out the use pattern for an appender.

## Implementation Notes

The implementation of the Spatial backend is located at [weld/spatial.rs](weld/spatial.rs), which
also contains more detailed documentation in comments.

The entrypoint to the Spatial backend is the `ast_to_spatial` function, which takes a Weld AST
and returns the generated Spatial code in text.  This function mostly just emits boilerplate and
initialization code, and then calls `gen_expr` on the Weld function body.  The `gen_expr` function
recursively pattern matches on and emits Spatial code for a Weld AST.

## Todo List

- [ ] Actually synthesize some generated Spatial code, load it onto an FPGA, and measure and tune
  performance.
- [ ] Find better Spatial implementations for `vecmerger` and the "filter" case of `appender`.
- [ ] Emit Spatial parameters for block sizes and the amount of parallelism.  They are currently
  emitted as constants (e.g., `Pipe(veclen by 16 par 4)`), which means Spatial's
  [design space exploration](http://spatial-lang.readthedocs.io/en/latest/tutorial/dse.html) cannot
  be exploited.
- [ ] Support `for` expressions with composite builders, e.g., `for(data, {appender[i32],
  merger[i32, +]}, ...)`.
- [ ] Improve support for structs (see the "Conditionals" section for an example).
