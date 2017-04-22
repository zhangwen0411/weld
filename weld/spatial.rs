//! Emits Spatial code for Weld AST.

use std::collections::HashMap;

use super::ast::*;
use super::code_builder::CodeBuilder;
use super::error::*;
use super::pretty_print::*;
use super::util::SymbolGenerator;

struct GlobalCtx {
    pub code: CodeBuilder,
    sym_gen: SymbolGenerator,
}

impl GlobalCtx {
    pub fn new(weld_ast: &TypedExpr) -> GlobalCtx {
        GlobalCtx {
            code: CodeBuilder::new(),
            sym_gen: SymbolGenerator::from_expression(weld_ast),
        }
    }

    pub fn add_variable(&mut self) -> Symbol {
        self.sym_gen.new_symbol("tmp")
    }
}

#[derive(Clone)]
struct MergerState {
    pub binop: BinOpKind,
    pub value: Symbol,  // Symbol that contains the current value of merger
}

#[derive(Clone)]
struct LocalCtx {
    veclen_sym: HashMap<Symbol, Symbol>, // symbol for vector => symbol for vector length
}

impl LocalCtx {
    pub fn new() -> LocalCtx {
        LocalCtx {
            veclen_sym: HashMap::new(),
        }
    }

    pub fn get_veclen_sym(&mut self, vec: &Symbol, glob_ctx: &mut GlobalCtx) -> Symbol {
        let res = self.veclen_sym.get(vec).map(Symbol::clone);
        match res {
            Some(len_sym) => len_sym,
            None => {
                let len_sym = glob_ctx.add_variable();
                self.veclen_sym.insert(vec.clone(), len_sym.clone());
                len_sym
            }
        }
    }
}

pub fn ast_to_spatial(expr: &TypedExpr) -> WeldResult<String> {
    if let ExprKind::Lambda { ref params, ref body } = expr.kind {
        let mut glob_ctx = GlobalCtx::new(expr);
        let mut local_ctx = LocalCtx::new();

        // Generate parameter list for Scala function.
        let mut scala_params = Vec::new();
        for param in params {
            let scala_type = gen_scala_param_type(&param.ty)?;
            scala_params.push(format!("param_{}: {}", gen_sym(&param.name), scala_type));
        }
        let scala_param_list = scala_params.join(", ");

        // Scala function definition.
        glob_ctx.code.add("@virtualize");
        glob_ctx.code.add(format!("def spatialProg({}) = {{", scala_param_list));

        // Set up input parameters for Spatial code.
        for param in params {
            let scala_param_name = format!("param_{}", gen_sym(&param.name));
            gen_spatial_input_param_setup(param, scala_param_name,
                                          &mut glob_ctx, &mut local_ctx)?;
        }

        // Set up output.
        let (output_prologue, output_body, output_epilogue) =
            gen_spatial_output_setup(&body.ty)?;
        glob_ctx.code.add(output_prologue);

        // Generate Spatial code (Accel block).
        glob_ctx.code.add("Accel {");
        let res_sym = gen_expr(body, &mut glob_ctx, &mut local_ctx)?;
        glob_ctx.code.add(output_body.replace("$RES_SYM", &gen_sym(&res_sym)));
        glob_ctx.code.add("}"); // Accel

        // Return value.
        glob_ctx.code.add(output_epilogue);
        glob_ctx.code.add("}"); // def spatialProg

        Ok(glob_ctx.code.result().to_string())
    } else {
        weld_err!("Expression passed to ast_to_spatial was not a Lambda")
    }
}

fn gen_scala_param_type(ty: &Type) -> WeldResult<String> {
    match *ty {
        Type::Scalar(scalar_kind) => Ok(gen_scalar_type_from_kind(scalar_kind)),

        Type::Vector(ref boxed_ty) => {
            let err_msg = format!("Not supported: nested type {}", print_type(ty));
            let elem_type_scala = gen_scalar_type(boxed_ty, err_msg)?;
            Ok(format!("Array[{}]", elem_type_scala))
        }

        _ => {
            weld_err!("Not supported: type {}", print_type(ty))
        }
    }
}

fn gen_spatial_input_param_setup(param: &Parameter<Type>,
                                 scala_param_name: String,
                                 glob_ctx: &mut GlobalCtx,
                                 local_ctx: &mut LocalCtx)
                                 -> WeldResult<()> {
    let spatial_param_name = gen_sym(&param.name);
    match param.ty {
        Type::Scalar(scalar_kind) => {
            glob_ctx.code.add(format!("val {} = ArgIn[{}]",
                                      spatial_param_name,
                                      gen_scalar_type_from_kind(scalar_kind)));
            glob_ctx.code.add(format!("setArg({}, {})", spatial_param_name, scala_param_name));
            Ok(())
        }

        Type::Vector(ref boxed_ty) => {
            let err_msg = format!("Not supported: nested type {}", print_type(&param.ty));
            let elem_type_scala = gen_scalar_type(boxed_ty, err_msg)?;

            let veclen_sym_name = gen_sym(&local_ctx.get_veclen_sym(&param.name, glob_ctx));
            glob_ctx.code.add(format!("val {} = ArgIn[Index]", veclen_sym_name));
            glob_ctx.code.add(format!("setArg({}, {}.length)",
                                      veclen_sym_name, scala_param_name));
            glob_ctx.code.add(format!("val {} = DRAM[{}]({})",
                                      spatial_param_name, elem_type_scala, veclen_sym_name));
            glob_ctx.code.add(format!("setMem({}, {})", spatial_param_name, scala_param_name));

            Ok(())
        }

        _ => {
            weld_err!("Not supported: type {}", print_type(&param.ty))
        }
    }
}

/// Returns (prologue, body, epilogue).
fn gen_spatial_output_setup(ty: &Type)
                            -> WeldResult<(String, String, String)> {
    match *ty {
        Type::Scalar(scalar_kind) => Ok((
            format!("val out = ArgOut[{}]", gen_scalar_type_from_kind(scalar_kind)),
            String::from("out := $RES_SYM"),
            String::from("getArg(out)")
        )),

        _ => {
            weld_err!("Not supported: result type {}", print_type(ty))
        }
    }
}

fn gen_scalar_type_from_kind(scalar_kind: ScalarKind) -> String {
    String::from(match scalar_kind {
        ScalarKind::Bool => "Boolean",
        ScalarKind::I8 => "Char",
        ScalarKind::I32 => "Int",
        ScalarKind::I64 => "Long",
        ScalarKind::F32 => "Float",
        ScalarKind::F64 => "Double",
    })
}

fn gen_scalar_type(ty: &Type, err_msg: String) -> WeldResult<String> {
    match *ty {
        Type::Scalar(scalar_kind) => Ok(gen_scalar_type_from_kind(scalar_kind)),
        _ => Err(WeldError::new(err_msg)),
    }
}

fn gen_expr(expr: &TypedExpr, glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx)
            -> WeldResult<Symbol> {
    match expr.kind {
        ExprKind::Ident(ref sym) => Ok(sym.clone()),

        ExprKind::Literal(lit) => {
            let res_sym = glob_ctx.add_variable();
            glob_ctx.code.add(format!("val {} = {}", gen_sym(&res_sym), gen_lit(lit)));
            Ok(res_sym)
        }

        ExprKind::BinOp { kind, ref left, ref right } => {
            let left_sym = gen_expr(left, glob_ctx, local_ctx)?;
            let right_sym = gen_expr(right, glob_ctx, local_ctx)?;
            let res_sym = glob_ctx.add_variable();
            glob_ctx.code.add(format!("val {} = {} {} {}", gen_sym(&res_sym),
                                      gen_sym(&left_sym), kind, gen_sym(&right_sym)));
            Ok(res_sym)
        }

        ExprKind::Negate(ref sub_expr) => {
            let sub_expr_sym = gen_expr(sub_expr, glob_ctx, local_ctx)?;
            let res_sym = glob_ctx.add_variable();
            glob_ctx.code.add(format!("val {} = -{}", gen_sym(&res_sym), gen_sym(&sub_expr_sym)));
            Ok(res_sym)
        }

        ExprKind::If { ref cond, ref on_true, ref on_false } => {
            let cond_sym = gen_expr(cond, glob_ctx, local_ctx)?;
            let mut true_local_ctx = local_ctx.clone();
            let on_true_sym = gen_expr(on_true, glob_ctx, &mut true_local_ctx)?;
            let mut false_local_ctx = local_ctx.clone();
            let on_false_sym = gen_expr(on_false, glob_ctx, &mut false_local_ctx)?;

            let res_sym = gen_mux(&cond_sym, &on_true_sym, &on_false_sym, glob_ctx);
            Ok(res_sym)
        }

        ExprKind::For { ref iters, ref builder, ref func } => {
            // iters
            if iters.len() != 1 {
                weld_err!("Not supported: for with multiple iters")?
            }
            let ref iter = iters[0];
            if iter.start.is_some() || iter.end.is_some() || iter.stride.is_some() {
                weld_err!("Not supported: iter with start, end, and/or stride")?
            }
            let data_res = gen_expr(&iter.data, glob_ctx, local_ctx)?;

            // builder
            let builder_sym = gen_expr(builder, glob_ctx, local_ctx)?;
            let builder_kind = match builder.ty {
                Type::Builder(ref kind) => kind,
                _ => weld_err!("Builder is not a builder: {}", print_expr(builder))?,
            };

            // func
            if let ExprKind::Lambda { ref params, ref body } = func.kind {
                let ref b_sym = params[0].name;
                let ref i_sym = params[1].name;
                let ref e_sym = params[2].name;

                match *builder_kind {
                    BuilderKind::Merger(ref elem_ty, binop_kind) => {
                        const BLK_SIZE: i32 = 16; // TODO(zhangwen): tunable parameter.
                        // TODO(zhangwen): currently must divide vector size evenly.
                        let veclen_sym = local_ctx.get_veclen_sym(&data_res, glob_ctx);
                        // The % operator doesn't work on registers; the `+0` is a hack
                        // to make it work.
                        glob_ctx.code.add(format!("assert(({}+0) % {} == 0)",
                                                  gen_sym(&veclen_sym),
                                                  BLK_SIZE));

                        let reduce_res = glob_ctx.add_variable();
                        let merger_type_scala = gen_scalar_type(elem_ty, format!(
                                "Not supported merger elem type: {}",
                                print_type(elem_ty as &Type)))?;
                        // TODO(zhangwen): couldn't get Fold to work in Spatial.
                        glob_ctx.code.add(
                            format!(
                                "val {reduce_res} = \
                                    Reduce(Reg[{elem_ty}])({size} by {blk_size}){{ i =>",
                                elem_ty=merger_type_scala,
                                reduce_res=gen_sym(&reduce_res),
                                size=gen_sym(&veclen_sym),
                                blk_size=BLK_SIZE,
                        ));

                        // Tiling: bring BLK_SIZE elements into SRAM.
                        let sram_sym = glob_ctx.add_variable();
                        glob_ctx.code.add(format!("val {} = SRAM[{}]({})",
                                                  gen_sym(&sram_sym),
                                                  merger_type_scala,
                                                  BLK_SIZE));
                        glob_ctx.code.add(format!("{} load {}(i::i+{})",
                                                  gen_sym(&sram_sym),
                                                  gen_sym(&data_res),
                                                  BLK_SIZE));
                        glob_ctx.code.add(format!("Reduce(Reg[{}])({} by 1){{ ii =>",
                                                  merger_type_scala,
                                                  BLK_SIZE));

                        // TODO(zhangwen): Spatial indices are 32-bit.
                        glob_ctx.code.add(format!("val {} = (i + ii).to[Long]", gen_sym(i_sym)));
                        glob_ctx.code.add(format!("val {} = {}(ii)",
                                                  gen_sym(e_sym), gen_sym(&sram_sym)));

                        // Entering a new scope.
                        let mut sub_local_ctx = local_ctx.clone();
                        // Create new merger for `b`; all local merges are gathered into it.
                        gen_new_merger(elem_ty, glob_ctx, Some(b_sym))?;

                        let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
                        glob_ctx.code.add(gen_sym(&body_res));
                        glob_ctx.code.add(format!("}}{{ _{}_ }}", binop_kind)); // Reduce
                        // Exiting scope.

                        glob_ctx.code.add(format!("}}{{ _{binop}_ }} {binop} {old_value}",
                                                  binop=binop_kind,
                                                  old_value=gen_sym(&builder_sym))); // Reduce
                        Ok(reduce_res)
                    }

                    _ => weld_err!("Builder kind not supported: {}", print_expr(builder))
                }
            } else {
                weld_err!("Argument to For was not a Lambda: {}", print_expr(func))
            }
        }

        ExprKind::NewBuilder(_) => {
            if let Type::Builder(ref kind) = expr.ty {
                match *kind {
                    BuilderKind::Merger(ref elem_ty, _) =>
                        gen_new_merger(elem_ty, glob_ctx, None),

                    _ => weld_err!("Builder kind not supported: {}", print_expr(expr))
                }
            } else {
                weld_err!("NewBuilder doesn't produce a builder: {}", print_expr(expr))
            }
        }

        ExprKind::Merge { ref builder, ref value } => {
            let builder_sym = gen_expr(builder, glob_ctx, local_ctx)?;
            let value_res = gen_expr(value, glob_ctx, local_ctx)?;

            if let Type::Builder(ref kind) = builder.ty {
                match *kind {
                    BuilderKind::Merger(_, binop) => {
                        let res_sym = glob_ctx.add_variable();
                        glob_ctx.code.add(format!("val {new_val} = {old_val} {op} {value}",
                              new_val=gen_sym(&res_sym),
                              old_val=gen_sym(&builder_sym),
                              op=binop,
                              value=gen_sym(&value_res)));
                        Ok(res_sym)
                    }

                    _ => weld_err!("Builder kind not supported: {}", print_expr(builder))
                }
            } else {
                weld_err!("Merging into not a builder: {}", print_expr(builder))
            }
        }

        ExprKind::Res { ref builder } => {
            if let Type::Builder(ref kind) = builder.ty {
                let builder_sym = gen_expr(builder, glob_ctx, local_ctx)?;
                match *kind {
                    BuilderKind::Merger(_, _) => Ok(builder_sym),
                    _ => weld_err!("Builder kind not supported: {}", print_expr(builder)),
                }
            } else {
                weld_err!("Res of not a builder: {}", print_expr(builder))
            }
        }

        _ => {
            weld_err!("Not supported: {}", print_expr(&expr))
        }
    }
}

fn gen_new_merger(elem_ty: &Type, glob_ctx: &mut GlobalCtx,
                  merger_sym: Option<&Symbol>) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported merger type: {}",
                                                           print_type(elem_ty)))?;
    let init_sym = merger_sym.map_or_else(|| glob_ctx.add_variable(), Symbol::clone);
    // TODO(zhangwen): this probably doesn't work for Boolean.
    // TODO(zhangwen): this also doesn't work for, say, multiplication.
    glob_ctx.code.add(format!("val {} = 0.to[{}]", gen_sym(&init_sym), elem_type_scala));
    Ok(init_sym)
}

/// Returns a symbol whose value is `cond_sym ? on_true_sym : on_false_sym`.
fn gen_mux(cond_sym: &Symbol, on_true_sym: &Symbol, on_false_sym: &Symbol,
           glob_ctx: &mut GlobalCtx) -> Symbol {
    if on_true_sym == on_false_sym {  // No need for mux (since no side effects).
        on_true_sym.clone()
    } else {
        let res_sym = glob_ctx.add_variable();
        glob_ctx.code.add(format!("val {} = mux({}, {}, {})",
                                  gen_sym(&res_sym),
                                  gen_sym(&cond_sym),
                                  gen_sym(&on_true_sym),
                                  gen_sym(&on_false_sym)));
        res_sym
    }
}

fn gen_lit(lit: LiteralKind) -> String {
    let (val_str, type_str) = match lit {
        LiteralKind::BoolLiteral(b) => (b.to_string(), "Boolean"),
        LiteralKind::I8Literal(i) => (i.to_string(), "Char"),
        LiteralKind::I32Literal(i) => (i.to_string(), "Int"),
        LiteralKind::I64Literal(i) => (i.to_string(), "Long"),
        LiteralKind::F32Literal(f) => (f.to_string(), "Float"),  // TODO(zhangwen): this works?
        LiteralKind::F64Literal(f) => (f.to_string(), "Double"),
    };
    format!("{}.to[{}]", val_str, type_str)
}

fn gen_sym(sym: &Symbol) -> String {
    format!("{}_{}", sym.name, sym.id)
}

