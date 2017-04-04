//! Emits Spatial code for Weld AST.

use std::collections::HashMap;

use super::ast::*;
use super::code_builder::CodeBuilder;
use super::error::*;
use super::pretty_print::*;
use super::util::SymbolGenerator;

pub struct SpatialProgram {
    pub code: CodeBuilder,
    veclen_sym: HashMap<Symbol, Symbol>,
    sym_gen: SymbolGenerator,
}

impl SpatialProgram {
    pub fn new(weld_ast: &TypedExpr) -> SpatialProgram {
        let prog = SpatialProgram {
            code: CodeBuilder::new(),
            veclen_sym: HashMap::new(),
            sym_gen: SymbolGenerator::from_expression(weld_ast),
        };
        prog
    }

    pub fn add_variable(&mut self) -> Symbol {
        self.sym_gen.new_symbol("tmp")
    }
    
    pub fn get_veclen_sym(&mut self, vec: &Symbol) -> Symbol {
        let res = self.veclen_sym.get(vec).map(Symbol::clone);
        match res {
            Some(len_sym) => len_sym,
            None => {
                let len_sym = self.add_variable();
                self.veclen_sym.insert(vec.clone(), len_sym.clone());
                len_sym
            }
        }
    }
}

pub fn ast_to_spatial(expr: &TypedExpr) -> WeldResult<String> {
    if let ExprKind::Lambda { ref params, ref body } = expr.kind {
        let mut spatial_prog = SpatialProgram::new(expr);

        // Generate parameter list for Scala function.
        let mut scala_params = Vec::new();
        for param in params {
            let scala_type = gen_scala_param_type(&param.ty)?;
            scala_params.push(format!("param_{}: {}", gen_sym(&param.name), scala_type));
        }
        let scala_param_list = scala_params.join(", ");

        // Scala function definition.
        spatial_prog.code.add("@virtualize");
        spatial_prog.code.add(format!("def spatialProg({}) = {{", scala_param_list));

        // Set up input parameters for Spatial code.
        for param in params {
            let scala_param_name = format!("param_{}", gen_sym(&param.name));
            gen_spatial_input_param_setup(param, scala_param_name, &mut spatial_prog)?;
        }

        // Set up output.
        let (output_prologue, output_body, output_epilogue) =
            gen_spatial_output_setup(&body.ty)?;
        spatial_prog.code.add(output_prologue);

        // Generate Spatial code (Accel block).
        spatial_prog.code.add("Accel {");
        let res_sym = gen_expr(body, &mut spatial_prog)?;
        spatial_prog.code.add(output_body.replace("$RES_SYM", &gen_sym(&res_sym)));
        spatial_prog.code.add("}"); // Accel

        // Return value.
        spatial_prog.code.add(output_epilogue);
        spatial_prog.code.add("}"); // def spatialProg

        Ok(spatial_prog.code.result().to_string())
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
                                 prog: &mut SpatialProgram)
                                 -> WeldResult<()> {
    let spatial_param_name = gen_sym(&param.name);
    match param.ty {
        Type::Scalar(scalar_kind) => {
            prog.code.add(format!("val {} = ArgIn[{}]",
                                  spatial_param_name,
                                  gen_scalar_type_from_kind(scalar_kind)));
            prog.code.add(format!("setArg({}, {})", spatial_param_name, scala_param_name));
            Ok(())
        }

        Type::Vector(ref boxed_ty) => {
            let err_msg = format!("Not supported: nested type {}", print_type(&param.ty));
            let elem_type_scala = gen_scalar_type(boxed_ty, err_msg)?;

            let veclen_sym_name = gen_sym(&prog.get_veclen_sym(&param.name));
            prog.code.add(format!("val {} = ArgIn[Int]", veclen_sym_name));
            prog.code.add(format!("setArg({}, {}.length)", veclen_sym_name, scala_param_name));
            prog.code.add(format!("val {} = DRAM[{}]({})",
                                  spatial_param_name,
                                  elem_type_scala,
                                  veclen_sym_name));
            prog.code.add(format!("setMem({}, {})", spatial_param_name, scala_param_name));

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
        ScalarKind::I8 => "Int", // TODO(zhangwen): other kinds of int?
        ScalarKind::I32 => "Int",
        ScalarKind::I64 => "Int", // TODO(zhangwen): how long is Int anyway?
        ScalarKind::F32 => "Float",
        ScalarKind::F64 => "Float", // TODO(zhangwen): how long is Float?
    })
}

fn gen_scalar_type(ty: &Type, err_msg: String) -> WeldResult<String> {
    match *ty {
        Type::Scalar(scalar_kind) => Ok(gen_scalar_type_from_kind(scalar_kind)),
        _ => Err(WeldError::new(err_msg)),
    }
}

fn gen_expr(expr: &TypedExpr, prog: &mut SpatialProgram)
            -> WeldResult<Symbol> {
    match expr.kind {
        ExprKind::Ident(ref sym) => Ok(sym.clone()),

        ExprKind::Literal(lit) => {
            let res_sym = prog.add_variable();
            prog.code.add(format!("val {} = {}", gen_sym(&res_sym), gen_lit(lit)));
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
            let data_res = gen_expr(&iter.data, prog)?;

            // builder
            match builder.ty {
                Type::Builder(ref builder_kind) => {
                    match *builder_kind {
                        BuilderKind::Merger(ref boxed_builder_ty, binop_kind) => {
                            let builder_res = gen_expr(builder, prog)?;

                            if let ExprKind::Lambda { ref params, ref body } = func.kind {
                                let ref b_sym = params[0].name;
                                let ref i_sym = params[1].name;
                                let ref e_sym = params[2].name;

                                if let ExprKind::Merge { ref builder, ref value } = body.kind {
                                    const BLK_SIZE: i32 = 16;
                                    // TODO(zhangwen): currently must divide vector size evenly.
                                    let veclen_sym = prog.get_veclen_sym(&data_res);
                                    // The % operator doesn't work on registers; the `+0` is a hack
                                    // to make it work.
                                    prog.code.add(format!("assert(({}+0) % {} == 0)",
                                                          gen_sym(&veclen_sym),
                                                          BLK_SIZE));

                                    prog.code.add(format!(
                                            "Reduce({acc})({size} by {blk_size}){{ i =>",
                                            acc=gen_sym(&builder_res),
                                            size=gen_sym(&veclen_sym),
                                            blk_size=BLK_SIZE,
                                    ));

                                    // Tiling: bring BLK_SIZE elements into SRAM.
                                    let merger_type_scala = gen_scalar_type(boxed_builder_ty,format!(
                                            "Not supported merger type: {}",
                                            print_type(boxed_builder_ty as &Type)))?;
                                    let sram_sym = prog.add_variable();
                                    prog.code.add(format!("val {} = SRAM[{}]({})",
                                                          gen_sym(&sram_sym),
                                                          merger_type_scala,
                                                          BLK_SIZE));
                                    prog.code.add(format!("{} load {}(i::i+{})",
                                                          gen_sym(&sram_sym),
                                                          gen_sym(&data_res),
                                                          BLK_SIZE));
                                    prog.code.add(format!("Reduce(Reg[{}])({} by 1){{ ii =>",
                                                          merger_type_scala,
                                                          BLK_SIZE));

                                    prog.code.add(format!("val {} = i*{} + ii",
                                                          gen_sym(i_sym), BLK_SIZE));
                                    prog.code.add(format!("val {} = {}(ii)",
                                                          gen_sym(e_sym),
                                                          gen_sym(&sram_sym)));

                                    let func_builder_sym = gen_expr(builder, prog)?;
                                    if func_builder_sym != *b_sym {
                                        weld_err!(
                                            "Not merging into designated builder: {}",
                                            print_expr(&expr))?;
                                    }
                                    let reduce_res = gen_expr(value, prog)?;
                                    prog.code.add(gen_sym(&reduce_res));
                                    prog.code.add(format!("}}{{ _{}_ }}", binop_kind)); // Reduce

                                    prog.code.add(format!("}}{{ _{}_ }}", binop_kind)); // Reduce
                                    Ok(builder_res)
                                } else {
                                    weld_err!("Not supported: For body not a merge")
                                }
                            } else {
                                weld_err!("Argument to For was not a Lambda: {}", print_expr(func))
                            }
                        }

                        _ => weld_err!("Not supported: {}", print_expr(builder))
                    }
                }

                _ => weld_err!("Argument to For was not a Builder: {}", print_expr(builder))
            }
        }

        ExprKind::NewBuilder(_) => {
            match expr.ty {
                Type::Builder(ref builder_kind) => {
                    match *builder_kind {
                        BuilderKind::Merger(ref boxed_builder_ty, _) => {
                            let elem_type_scala =
                                gen_scalar_type(boxed_builder_ty, format!(
                                        "Not supported merger type: {}",
                                        print_type(boxed_builder_ty as &Type)))?;
                            let reg_sym = prog.add_variable();
                            prog.code.add(format!("val {} = Reg[{}]",
                                                  gen_sym(&reg_sym),
                                                  elem_type_scala));
                            Ok(reg_sym)
                        }

                        _ => weld_err!("Not supported: {}", print_expr(&expr))
                    }
                }

                _ => weld_err!("Builder is not a builder: {}", print_expr(&expr))
            }
        }

        ExprKind::Res { ref builder } => {
            match expr.ty {
                Type::Scalar(_) => {
                    let reg_sym = gen_expr(builder, prog)?;
                    let res_sym = prog.add_variable();
                    prog.code.add(format!("val {} = {}.value",
                                          gen_sym(&res_sym),
                                          gen_sym(&reg_sym)));
                    Ok(res_sym)
                }

                _ => weld_err!("Not supported: {}", print_expr(&expr))
            }
        }

        _ => {
            weld_err!("Not supported: {}", print_expr(&expr))
        }
    }
}

fn gen_lit(lit: LiteralKind) -> String {
    match lit {
        LiteralKind::BoolLiteral(b) => b.to_string(),
        LiteralKind::I8Literal(i) => i.to_string(),
        LiteralKind::I32Literal(i) => i.to_string(),
        LiteralKind::I64Literal(i) => i.to_string(),
        LiteralKind::F32Literal(f) => f.to_string(),  // TODO(zhangwen): does this work in Scala?
        LiteralKind::F64Literal(f) => f.to_string(),
    }
}

fn gen_sym(sym: &Symbol) -> String {
    format!("{}_{}", sym.name, sym.id)
}

