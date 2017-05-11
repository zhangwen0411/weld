//! Emits Spatial code for Weld AST.

use std::collections::HashMap;

use super::ast::*;
use super::code_builder::CodeBuilder;
use super::error::*;
use super::pretty_print::*;
use super::util::SymbolGenerator;

struct GlobalCtx {
    sym_gen: SymbolGenerator,
    code_builders: Vec<CodeBuilder>,
    pub additional_init: CodeBuilder,
}

impl GlobalCtx {
    pub fn new(weld_ast: &TypedExpr) -> GlobalCtx {
        GlobalCtx {
            code_builders: Vec::new(),
            sym_gen: SymbolGenerator::from_expression(weld_ast),

            // Additional code added before Accel block.
            additional_init: CodeBuilder::new(),
        }
    }

    pub fn enter_builder_scope(&mut self) {
        self.code_builders.push(CodeBuilder::new());
    }

    pub fn exit_builder_scope(&mut self) -> String {
        self.code_builders.pop().unwrap().result().to_string()
    }

    pub fn add_variable(&mut self) -> Symbol {
        self.sym_gen.new_symbol("tmp")
    }

    pub fn add_code<S>(&mut self, code: S) where S: AsRef<str> {
        self.code_builders.last_mut().unwrap().add(code);
    }
}

macro_rules! add_code {
    ( $glob_ctx:expr, $($arg:tt)* ) => ({
        $glob_ctx.add_code(format!($($arg)*))
    })
}

#[derive(Clone)]
struct VecMergerState {
    sram_sym: Symbol,
    range_start_sym: Symbol,
    range_size: i32,
}

#[derive(Clone, PartialEq)]
enum AppenderState {
    Fresh, // Hasn't been used
    Map { sram_sym: Symbol, ii_sym: Symbol }, // Is being used in a map operation
    Filter, // Is being used in a filter operation
    Dead, // Can no longer be used
    // For now there's no invalid state because an unsupported operation terminates compilation.
}

#[derive(Debug, PartialEq)]
enum AppenderUsage {
    Unused,
    Once,
    AtMostOnce,
    Unsupported,
}

impl AppenderUsage {
    fn compose_seq(self, other: AppenderUsage) -> AppenderUsage {
        use self::AppenderUsage::*;
        match (self, other) {
            (u @ _, Unused) | (Unused, u @ _) => u,
            _ => Unsupported,
        }
    }

    fn compose_or(self, other: AppenderUsage) -> AppenderUsage {
        use self::AppenderUsage::*;
        match (self, other) {
            (Unused, Once) | (Once, Unused) => AtMostOnce,
            (Once, Once) => Once,
            (Unused, Unused) => Unused,
            (Unused, AtMostOnce) | (AtMostOnce, Unused) => AtMostOnce,
            _ => Unsupported,
        }
    }
}

#[derive(Clone)]
struct LocalCtx {
    veclen_sym: HashMap<Symbol, Symbol>, // symbol for vector => symbol for vector length
    vecmergers: HashMap<Symbol, VecMergerState>,
    pub appenders: HashMap<Symbol, AppenderState>,
    pub structs: HashMap<Symbol, Box<Vec<Symbol>>>, // symbol for struct value => components
}

impl LocalCtx {
    pub fn new() -> LocalCtx {
        LocalCtx {
            veclen_sym: HashMap::new(),
            vecmergers: HashMap::new(),
            appenders: HashMap::new(),
            structs: HashMap::new(),
        }
    }

    // Two vectors of the same length (statically) should share a length symbol.
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

    pub fn set_veclen_sym(&mut self, vec: &Symbol, len_sym: &Symbol) {
        self.veclen_sym.insert(vec.clone(), len_sym.clone());
    }

    pub fn set_vecmerger_state(&mut self, vecmerger_sym: &Symbol, sram_sym: &Symbol,
                               range_start_sym: &Symbol, range_size: i32) {
        self.vecmergers.insert(vecmerger_sym.clone(), VecMergerState {
            sram_sym: sram_sym.clone(),
            range_start_sym: range_start_sym.clone(),
            range_size: range_size,
        });
    }

    pub fn get_vecmerger_state(&self, vecmerger_sym: &Symbol) -> Option<VecMergerState> {
        self.vecmergers.get(vecmerger_sym).map(VecMergerState::clone)
    }
}

pub fn ast_to_spatial(expr: &TypedExpr) -> WeldResult<String> {
    if let ExprKind::Lambda { ref params, ref body } = expr.kind {
        let mut glob_ctx = GlobalCtx::new(expr);
        glob_ctx.enter_builder_scope();
        let mut local_ctx = LocalCtx::new();

        // Generate parameter list for Scala function.
        let mut scala_params = Vec::new();
        for param in params {
            let scala_type = gen_scala_param_type(&param.ty)?;
            scala_params.push(format!("param_{}: {}", gen_sym(&param.name), scala_type));
        }
        let scala_param_list = scala_params.join(", ");

        // Scala function definition.
        add_code!(glob_ctx, "@virtualize");
        add_code!(glob_ctx, "def spatialProg({}) = {{", scala_param_list);

        // Set up input parameters for Spatial code.
        for param in params {
            let scala_param_name = format!("param_{}", gen_sym(&param.name));
            gen_spatial_input_param_setup(param, scala_param_name,
                                          &mut glob_ctx, &mut local_ctx)?;
        }

        // Set up output.
        let (output_prologue, output_body, output_epilogue) =
            gen_spatial_output_setup(&body.ty)?;
        add_code!(glob_ctx, "{}", output_prologue);

        glob_ctx.enter_builder_scope();
        let res_sym = gen_expr(body, &mut glob_ctx, &mut local_ctx)?;
        let accel_body = glob_ctx.exit_builder_scope();

        let additional_init_code = glob_ctx.additional_init.result().to_string();
        add_code!(glob_ctx, "{}", additional_init_code);

        // Generate Spatial code (Accel block).
        add_code!(glob_ctx, "Accel {{");
        add_code!(glob_ctx, "Sequential {{");
        add_code!(glob_ctx, "{}", accel_body);
        add_code!(glob_ctx, "{}", output_body.replace("$RES_SYM", &gen_sym(&res_sym)));
        add_code!(glob_ctx, "}}"); // Sequential
        add_code!(glob_ctx, "}}"); // Accel

        // Return value.
        add_code!(glob_ctx, "{}", output_epilogue.replace("$RES_SYM", &gen_sym(&res_sym)));
        add_code!(glob_ctx, "}}"); // def spatialProg

        Ok(glob_ctx.exit_builder_scope())
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
            add_code!(glob_ctx, "val {} = ArgIn[{}]", spatial_param_name,
                      gen_scalar_type_from_kind(scalar_kind));
            add_code!(glob_ctx, "setArg({}, {})", spatial_param_name, scala_param_name);
            Ok(())
        }

        Type::Vector(ref boxed_ty) => {
            let err_msg = format!("Not supported: nested type {}", print_type(&param.ty));
            let elem_type_scala = gen_scalar_type(boxed_ty, err_msg)?;

            let veclen_sym_name = gen_sym(&local_ctx.get_veclen_sym(&param.name, glob_ctx));
            add_code!(glob_ctx, "val {} = ArgIn[Index]", veclen_sym_name);
            add_code!(glob_ctx, "setArg({}, {}.length)", veclen_sym_name, scala_param_name);
            add_code!(glob_ctx, "val {} = DRAM[{}]({})",
                      spatial_param_name, elem_type_scala, veclen_sym_name);
            add_code!(glob_ctx, "setMem({}, {})", spatial_param_name, scala_param_name);

            Ok(())
        }

        _ => weld_err!("Not supported: type {}", print_type(&param.ty))
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

        Type::Vector(ref boxed_ty) => {
            let err_msg = format!("Not supported: nested type {}", print_type(ty));
            gen_scalar_type(boxed_ty, err_msg)?;
            Ok((String::new(), String::new(), String::from("getMem($RES_SYM)")))
        }

        _ => weld_err!("Not supported: result type {}", print_type(ty))
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
            add_code!(glob_ctx, "val {} = {}", gen_sym(&res_sym), gen_lit(lit));
            Ok(res_sym)
        }

        ExprKind::BinOp { kind, ref left, ref right } => {
            let left_sym = gen_expr(left, glob_ctx, local_ctx)?;
            let right_sym = gen_expr(right, glob_ctx, local_ctx)?;
            let res_sym = glob_ctx.add_variable();
            add_code!(glob_ctx, "val {} = {} {} {}", gen_sym(&res_sym),
                      gen_sym(&left_sym), kind, gen_sym(&right_sym));
            Ok(res_sym)
        }

        ExprKind::Negate(ref sub_expr) => {
            let sub_expr_sym = gen_expr(sub_expr, glob_ctx, local_ctx)?;
            let res_sym = glob_ctx.add_variable();
            add_code!(glob_ctx, "val {} = -{}", gen_sym(&res_sym), gen_sym(&sub_expr_sym));
            Ok(res_sym)
        }

        ExprKind::If { ref cond, ref on_true, ref on_false } => {
            let cond_sym = gen_expr(cond, glob_ctx, local_ctx)?;

            let mut true_local_ctx = local_ctx.clone();
            glob_ctx.enter_builder_scope();
            let on_true_sym = gen_expr(on_true, glob_ctx, &mut true_local_ctx)?;
            let on_true_code = glob_ctx.exit_builder_scope();

            let mut false_local_ctx = local_ctx.clone();
            glob_ctx.enter_builder_scope();
            let on_false_sym = gen_expr(on_false, glob_ctx, &mut false_local_ctx)?;
            let on_false_code = glob_ctx.exit_builder_scope();

            if is_spatial_mutable(&expr.ty) {
                if on_true_sym != on_false_sym {
                    weld_err!("Not supported: if branches return different things {}",
                              print_expr(expr))
                } else {
                    // In this case, the symbol must have been defined outside the `if`.
                    add_code!(glob_ctx, "\
                        if ({cond}) {{
                            Sequential {{
                                {on_true_code}
                            }}
                        }} else {{
                            Sequential {{
                                {on_false_code}
                            }}
                        }}",

                        cond=gen_sym(&cond_sym),
                        on_true_code=on_true_code,
                        on_false_code=on_false_code);

                    Ok(on_true_sym)
                }
            } else {
                // Mux the two values!
                add_code!(glob_ctx, "{}", on_true_code);
                add_code!(glob_ctx, "{}", on_false_code);
                let res_sym = gen_mux(&cond_sym, &on_true_sym, &on_false_sym, glob_ctx);
                Ok(res_sym)
            }
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
            let data_ty = match iter.data.ty {
                Type::Vector(ref ty) => ty,
                _ => weld_err!("Iter doesn't have a vector: {}", print_expr(expr))?
            };

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
                    BuilderKind::Merger(ref elem_ty, binop) =>
                        gen_merger_loop(&data_res, elem_ty, body, &builder_sym, binop,
                                        b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                    BuilderKind::VecMerger(ref elem_ty, binop) =>
                        gen_vecmerger_loop(&data_res, data_ty, elem_ty, body, &builder_sym, binop,
                                           b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                    BuilderKind::Appender(ref elem_ty) => {
                        // Prohibit the appender from being used again.
                        let old_state = local_ctx.appenders.insert(builder_sym.clone(),
                                                                   AppenderState::Dead).unwrap();
                        // Make sure that the appender was fresh.
                        if old_state != AppenderState::Fresh {
                            weld_err!("Appender {} reused: {}", builder_sym, print_expr(expr))?;
                        }

                        let (usage, ret) = compute_appender_usage(body, b_sym);
                        if !ret {
                            weld_err!("For loop body doesn't return builder: {}",
                                      print_expr(expr))?;
                        }

                        match usage {
                            // Assuming that the for loop can only merge into the builder
                            // provided to it, that would be its only possible side effect.
                            // If such merge doesn't happen, why bother generating code for the
                            // loop body?
                            AppenderUsage::Unused => Ok(builder_sym),

                            AppenderUsage::Once =>
                                gen_map_loop(&data_res, elem_ty, body, &builder_sym,
                                             b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                            AppenderUsage::AtMostOnce =>
                                gen_filter_loop(&data_res, elem_ty, body, &builder_sym,
                                                b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                            AppenderUsage::Unsupported =>
                                weld_err!("Unsupported appender usage {:?}: {}",
                                          usage, print_expr(expr)),
                        }
                    }

                    _ => weld_err!("For: builder kind not supported: {}", print_expr(expr))
                }
            } else {
                weld_err!("Argument to For was not a Lambda: {}", print_expr(func))
            }
        }

        ExprKind::NewBuilder(ref builder_expr) => {
            if let Type::Builder(ref kind) = expr.ty {
                match *kind {
                    BuilderKind::Merger(ref elem_ty, _) =>
                        gen_new_merger(elem_ty, glob_ctx, None),

                    BuilderKind::VecMerger(ref elem_ty, _) => {
                        let vec_sym = gen_expr(builder_expr.as_ref().unwrap(),
                                               glob_ctx, local_ctx)?;
                        gen_new_vecmerger(&vec_sym, elem_ty, glob_ctx, local_ctx)
                    }

                    BuilderKind::Appender(_) => gen_new_appender(glob_ctx, local_ctx),

                    _ => weld_err!("NewBuilder: builder kind not supported: {}", print_expr(expr))
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
                        add_code!(glob_ctx, "val {new_val} = {old_val} {op} {value}",
                              new_val=gen_sym(&res_sym),
                              old_val=gen_sym(&builder_sym),
                              op=binop,
                              value=gen_sym(&value_res));
                        Ok(res_sym)
                    }

                    BuilderKind::VecMerger(_, binop) => {
                        let vecmerger_state = match local_ctx.get_vecmerger_state(&builder_sym) {
                            Some(state) => state,
                            None =>
                                weld_err!("Not supported: vecmerger merge {}", print_expr(expr))?
                        };
                        let values = local_ctx.structs.get(&value_res).unwrap();
                        let ref index_sym = values[0];
                        let ref merge_val_sym = values[1];

                        // If the index is in range, merge the value.
                        let offset_sym = glob_ctx.add_variable();
                        let offset_sym_name = gen_sym(&offset_sym);
                        // TODO(zhangwen): will break if index overflows 32-bit int.
                        add_code!(glob_ctx, "\
                                  val {offset} = {index}.to[Index] - {start}
                                  if ({offset} >= 0 && {offset} < {range_size}) {{
                                      {sram}({offset}) = {sram}({offset}) {binop} {value}
                                  }}",
                                  offset        = offset_sym_name,
                                  index         = gen_sym(index_sym),
                                  start         = gen_sym(&vecmerger_state.range_start_sym),
                                  range_size    = vecmerger_state.range_size,
                                  sram          = gen_sym(&vecmerger_state.sram_sym),
                                  binop         = binop,
                                  value         = gen_sym(merge_val_sym)
                        );
                        Ok(builder_sym)
                    }

                    BuilderKind::Appender(_) => {
                        use self::AppenderState::*;
                        let appender_state = local_ctx.appenders.get(&builder_sym).unwrap();
                        match *appender_state {
                            Map { ref sram_sym, ref ii_sym } => {
                                add_code!(glob_ctx, "{sram}({ii}) = {value}",
                                          sram=gen_sym(&sram_sym),
                                          ii=gen_sym(&ii_sym),
                                          value=gen_sym(&value_res));
                                Ok(builder_sym)
                            }
                            Filter => weld_err!("Not implemented: filter merge"),
                            _ => weld_err!("Merge not supported: {}", print_expr(expr)),
                        }
                    }

                    _ => weld_err!("Merge: builder kind not supported: {}", print_expr(expr))
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
                    BuilderKind::VecMerger(_, _) => Ok(builder_sym),
                    BuilderKind::Appender(_) => Ok(builder_sym),
                    _ => weld_err!("Res: builder kind not supported: {}", print_expr(expr)),
                }
            } else {
                weld_err!("Res of not a builder: {}", print_expr(builder))
            }
        }

        ExprKind::MakeStruct { ref elems } => {
            let mut res_syms: Vec<Symbol> = Vec::new();
            for child_expr in elems {
                let res_sym = gen_expr(child_expr, glob_ctx, local_ctx)?;
                res_syms.push(res_sym);
            }
            let struct_sym = glob_ctx.add_variable(); // Just an identifier for the struct.
            local_ctx.structs.insert(struct_sym.clone(), Box::new(res_syms));
            Ok(struct_sym)
        }

        _ => {
            weld_err!("Not supported: {}", print_expr(&expr))
        }
    }
}

// FIXME(zhangwen): side effects should respect conditionals!!!
fn gen_vecmerger_loop(data_sym: &Symbol, data_ty: &Type, dst_ty: &Type, body: &TypedExpr,
                      dst_sym: &Symbol, binop: BinOpKind,
                      b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                      glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx)
                      -> WeldResult<Symbol> {
    const BLK_SIZE_DST: i32 = 16; // TODO(zhangwen): tunable parameter.
    const BLK_SIZE_SRC: i32 = 16; // TODO(zhangwen): tunable parameter.
    const PAR: i32 = 4;
    let dst_veclen_sym = local_ctx.get_veclen_sym(dst_sym, glob_ctx);

    // Merge into blocks of vecmerger, one by one.
    let dst_ty_scala = gen_scalar_type(dst_ty, format!(
            "Not supported vecmerger elem type: {}", print_type(dst_ty)))?;
    let data_ty_scala = gen_scalar_type(data_ty, format!(
            "Not supported data vec elem type: {}", print_type(data_ty)))?;
    let dst_sram_sym = glob_ctx.add_variable();
    let range_start_sym = glob_ctx.add_variable();
    let data_len_sym = local_ctx.get_veclen_sym(data_sym, glob_ctx);
    let src_sram_sym = glob_ctx.add_variable();
    let local_dst_sram_sym = glob_ctx.add_variable();

    // Generate body code.
    let mut sub_local_ctx = local_ctx.clone();
    sub_local_ctx.set_vecmerger_state(&b_sym, &local_dst_sram_sym,
                                      &range_start_sym, BLK_SIZE_DST);
    glob_ctx.enter_builder_scope();
    let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
    assert_eq!(body_res, *b_sym); // Body should return builder derived from `b`.
    let body_code = glob_ctx.exit_builder_scope();

    // TODO(zhangwen): currently must divide vector size evenly.
    // FIXME(zhangwen): doesn't work for, say, multiplication.
    add_code!(glob_ctx, "\
        assert(({dst_len}+0) % {dst_blk} == 0)
        // Bring a block of destination DRAM into SRAM.
        Pipe({dst_len} by {dst_blk}) {{ {rs} =>
            val {dst_sram} = SRAM[{ty}]({dst_blk})
            {dst_sram} load {dst_dram}({rs}::{rs}+{dst_blk})

            assert(({data_len}+0) % {par} == 0)
            val piece_len = {data_len}/{par}
            assert(piece_len % {data_blk} == 0)
            MemFold({dst_sram})({data_len} by piece_len par {par}) {{ i1 =>
                val {local_dst_sram} = SRAM[{ty}]({dst_blk})
                Foreach({data_blk} by 1) {{ ii => {local_dst_sram}(ii) = 0 }}

                Pipe(piece_len by {data_blk}) {{ i2 =>
                    val base = i1 + i2
                    val {data_sram} = SRAM[{data_ty}]({data_blk})
                    {data_sram} load {data_dram}(base::base+{data_blk})

                    Pipe({data_blk} by 1) {{ i3 =>
                        // TODO(zhangwen): Spatial indices are 32-bit.
                        val {i_sym} = (base + i3).to[Long]
                        val {e_sym} = {data_sram}(i3)

                        Sequential {{
                            {body_code}
                        }}
                    }}  // Pipe
                }}  // Foreach

                {local_dst_sram}
            }} {{ _{binop}_ }}  // MemReduce

            {dst_dram}({rs}::{rs}+{dst_blk}) store {dst_sram}
        }}  // Pipe",
        dst_len         = gen_sym(&dst_veclen_sym),
        dst_blk         = BLK_SIZE_DST,
        par             = PAR,
        rs              = gen_sym(&range_start_sym),
        dst_sram        = gen_sym(&dst_sram_sym),
        dst_dram        = gen_sym(&dst_sym),
        ty              = dst_ty_scala,
        data_ty         = data_ty_scala,
        data_len        = gen_sym(&data_len_sym),
        data_blk        = BLK_SIZE_SRC,
        data_sram       = gen_sym(&src_sram_sym),
        data_dram       = gen_sym(&data_sym),
        local_dst_sram  = gen_sym(&local_dst_sram_sym),
        i_sym           = gen_sym(i_sym),
        e_sym           = gen_sym(e_sym),
        body_code       = body_code,
        binop           = binop);

    Ok(dst_sym.clone())
}

fn gen_merger_loop(data_sym: &Symbol, elem_ty: &Type, body: &TypedExpr,
                   merger_sym: &Symbol, binop: BinOpKind,
                   b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                   glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    const BLK_SIZE: i32 = 16; // TODO(zhangwen): tunable parameter.
    // TODO(zhangwen): currently must divide vector size evenly.
    let veclen_sym = local_ctx.get_veclen_sym(&data_sym, glob_ctx);
    let reduce_res = glob_ctx.add_variable();
    let merger_type_scala = gen_scalar_type(elem_ty, format!(
            "Not supported merger elem type: {}", print_type(elem_ty)))?;
    let sram_sym = glob_ctx.add_variable();

    // Generate code for body.
    glob_ctx.enter_builder_scope();
    let mut sub_local_ctx = local_ctx.clone();
    // Create new merger for `b`; all local merges are gathered into it.
    gen_new_merger(elem_ty, glob_ctx, Some(b_sym))?;
    let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
    let body_code = glob_ctx.exit_builder_scope();

    // TODO(zhangwen): Spatial indices are 32-bit.
    // FIXME(zhangwen): do I need Sequential around body_code?
    add_code!(glob_ctx, "\
        // The % operator doesn't work on registers; the `+0` is a hack
        // to make it work.
        assert(({veclen}+0) % {blk} == 0)
        val {reduce_res} = Reduce(Reg[{ty}])({veclen} by {blk}){{ i =>
            // Tiling: bring BLK_SIZE elements into SRAM.
            val {sram} = SRAM[{ty}]({blk})
            {sram} load {dram}(i::i+{blk})
            Reduce(Reg[{ty}])({blk} by 1){{ ii =>
                val {i_sym} = (i + ii).to[Long]
                val {e_sym} = {sram}(ii)

                {body_code}
                {body_res}
            }} {{ _{binop}_ }}  // Reduce
        }} {{ _{binop}_ }} {binop} {init_value}  // Reduce",

        veclen      = gen_sym(&veclen_sym),
        blk         = BLK_SIZE,
        reduce_res  = gen_sym(&reduce_res),
        ty          = merger_type_scala,
        sram        = gen_sym(&sram_sym),
        dram        = gen_sym(data_sym),
        i_sym       = gen_sym(i_sym),
        e_sym       = gen_sym(e_sym),
        body_code   = body_code,
        body_res    = gen_sym(&body_res),
        binop       = binop,
        init_value  = gen_sym(&merger_sym));

    Ok(reduce_res)
}


fn gen_map_loop(data_sym: &Symbol, elem_ty: &Type, body: &TypedExpr,
                appender_sym: &Symbol, b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported appender type: {}",
                                                           print_type(elem_ty)))?;
    let veclen_sym = local_ctx.get_veclen_sym(data_sym, glob_ctx);
    // The result of a map operation has the same length as the data.
    glob_ctx.additional_init.add(format!("val {} = DRAM[{}]({})",
              gen_sym(&appender_sym), elem_type_scala, gen_sym(&veclen_sym)));
    local_ctx.set_veclen_sym(&appender_sym, &veclen_sym);

    let sram_dst_sym = glob_ctx.add_variable();
    let ii_sym = glob_ctx.add_variable();

    // Generate body code.
    let mut sub_local_ctx = local_ctx.clone();
    sub_local_ctx.appenders.insert(
        b_sym.clone(),
        AppenderState::Map { sram_sym: sram_dst_sym.clone(), ii_sym: ii_sym.clone() });
    glob_ctx.enter_builder_scope();
    let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
    assert_eq!(body_res, *b_sym); // Body should return builder derived from `b`.
    let body_code = glob_ctx.exit_builder_scope();

    const BLK_SIZE: i32 = 16;
    add_code!(glob_ctx, "\
        assert(({veclen}+0) % {blk} == 0)
        Pipe({veclen} by {blk}) {{ i =>
            val sram_data = SRAM[{ty}]({blk})
            val {sram_dst} = SRAM[{ty}]({blk})
            sram_data load {data}(i::i+{blk})
            Foreach({blk} by 1) {{ {ii} =>
                val {i_sym} = (i + {ii}).to[Long]
                val {e_sym} = sram_data({ii})

                Sequential {{
                    {body_code}
                }}
            }}
            {dst}(i::i+{blk}) store {sram_dst}
        }}",

        veclen      = gen_sym(&veclen_sym),
        blk         = BLK_SIZE,
        ty          = elem_type_scala,
        sram_dst    = gen_sym(&sram_dst_sym),
        data        = gen_sym(&data_sym),
        ii          = gen_sym(&ii_sym),
        i_sym       = gen_sym(&i_sym),
        e_sym       = gen_sym(&e_sym),
        body_code   = body_code,
        dst         = gen_sym(&appender_sym));

    Ok(appender_sym.clone())
}

#[allow(unused_variables)]
fn gen_filter_loop(data_sym: &Symbol, elem_ty: &Type, body: &TypedExpr,
                   appender_sym: &Symbol, b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                   glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    weld_err!("Not implemented: filter loop")
    // TODO(zhangwen): implement this.
}

fn gen_new_vecmerger(vec_sym: &Symbol, elem_ty: &Type, glob_ctx: &mut GlobalCtx,
                     local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported vecmerger type: {}",
                                                           print_type(elem_ty)))?;
    let vecmerger_sym = glob_ctx.add_variable();
    let veclen_sym = local_ctx.get_veclen_sym(vec_sym, glob_ctx);
    glob_ctx.additional_init.add(format!("val {} = DRAM[{}]({})",
              gen_sym(&vecmerger_sym), elem_type_scala, gen_sym(&veclen_sym)));
    local_ctx.set_veclen_sym(&vecmerger_sym, &veclen_sym);

    // Copy from `vec_sym` to `vecmerger_sym`.
    // FIXME(zhangwen): do this lazily.
    const BLK_SIZE: i32 = 16; // TODO(zhangwen): tunable parameter.
    add_code!(glob_ctx, "\
        Pipe({veclen} by {blk}) {{ i =>
            val ss = SRAM[{ty}]({blk})
            ss load {vec}(i::i+{blk})
            {vecmerger}(i::i+{blk}) store ss
        }} // Pipe",

        veclen      = gen_sym(&veclen_sym),
        blk         = BLK_SIZE,
        ty          = elem_type_scala,
        vec         = gen_sym(&vec_sym),
        vecmerger   = gen_sym(&vecmerger_sym));

    Ok(vecmerger_sym)
}

fn gen_new_appender(glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let appender_sym = glob_ctx.add_variable();
    // Don't declare underlying DRAM yet.
    local_ctx.appenders.insert(appender_sym.clone(), AppenderState::Fresh);
    Ok(appender_sym)
}

fn gen_new_merger(elem_ty: &Type, glob_ctx: &mut GlobalCtx,
                  merger_sym: Option<&Symbol>) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported merger type: {}",
                                                           print_type(elem_ty)))?;
    let init_sym = merger_sym.map_or_else(|| glob_ctx.add_variable(), Symbol::clone);
    // TODO(zhangwen): this probably doesn't work for Boolean.
    // TODO(zhangwen): this also doesn't work for, say, multiplication.
    add_code!(glob_ctx, "val {} = 0.to[{}]", gen_sym(&init_sym), elem_type_scala);
    Ok(init_sym)
}

/// Returns a symbol whose value is `cond_sym ? on_true_sym : on_false_sym`.
fn gen_mux(cond_sym: &Symbol, on_true_sym: &Symbol, on_false_sym: &Symbol,
           glob_ctx: &mut GlobalCtx) -> Symbol {
    if on_true_sym == on_false_sym {  // No need for mux (since no side effects).
        on_true_sym.clone()
    } else {
        let res_sym = glob_ctx.add_variable();
        add_code!(glob_ctx, "val {} = mux({}, {}, {})", gen_sym(&res_sym), gen_sym(&cond_sym),
                  gen_sym(&on_true_sym), gen_sym(&on_false_sym));
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

/// Returns true if a Spatial variable for a Weld value of type `ty` is mutable, e.g. for
/// vecmerger.
fn is_spatial_mutable(ty: &Type) -> bool {
    match *ty {
        Type::Builder(ref kind) => {
            match *kind {
                BuilderKind::Appender(_) => true,
                BuilderKind::DictMerger(_, _, _) => true,
                BuilderKind::VecMerger(_, _) => true,
                _ => false,
            }
        }

        _ => false
    }
}

/// Returns appender usage, and true if expr returns the same appender.
fn compute_appender_usage(expr: &TypedExpr, appender_sym: &Symbol) -> (AppenderUsage, bool) {
    match expr.kind {
        ExprKind::Literal(_) => (AppenderUsage::Unused, false),

        ExprKind::Ident(ref sym) => (AppenderUsage::Unused, sym == appender_sym),

        ExprKind::BinOp { ref left, ref right, .. } => {
            let (left_usage, left_ret) = compute_appender_usage(left, appender_sym);
            let (right_usage, right_ret) = compute_appender_usage(right, appender_sym);
            assert!(!left_ret); // BinOp operands cannot be appenders.
            assert!(!right_ret);
            (left_usage.compose_seq(right_usage), false)
        }

        ExprKind::If { ref cond, ref on_true, ref on_false } => {
            let (cond_usage, cond_ret) = compute_appender_usage(cond, appender_sym);
            assert!(!cond_ret);  // A bool cannot be an appender.
            let (true_usage, true_ret) = compute_appender_usage(on_true, appender_sym);
            let (false_usage, false_ret) = compute_appender_usage(on_false, appender_sym);
            if true_ret != false_ret {
                // Unsupported if only one branch returns the appender in question.
                (AppenderUsage::Unsupported, false)
            } else {
                (cond_usage.compose_seq(true_usage.compose_or(false_usage)), true_ret)
            }
        }

        ExprKind::NewBuilder(ref opt) => {
            if let Some(ref e) = *opt {
                let (usage, ret) = compute_appender_usage(e, appender_sym);
                assert!(!ret);
                (usage, false)
            } else {
                (AppenderUsage::Unused, false)
            }
        }

        ExprKind::For { ref iters, ref builder, ref func } => {
            let mut usage = AppenderUsage::Unused;
            for iter in iters {
                let (iter_usage, iter_ret) = compute_appender_usage(iter.data.as_ref(),
                                                                    appender_sym);
                assert!(!iter_ret);  // A vector cannot be an appender.
                usage = usage.compose_seq(iter_usage)
            }

            let (builder_usage, builder_ret) = compute_appender_usage(builder, appender_sym);
            usage = usage.compose_seq(builder_usage);
            if builder_ret {  // For loop might merge into appender in question.
                if let ExprKind::Lambda { ref params, ref body } = func.kind {
                    let ref b_sym = params[0].name;
                    // In func, b_sym refers to the loop builder.
                    let (func_usage, func_ret) = compute_appender_usage(body, b_sym);
                    assert!(func_ret);  // Function body must return b_sym.
                    if func_usage != AppenderUsage::Unused { usage = AppenderUsage::Unsupported; }
                    (usage, true)
                } else {
                    panic!("For loop func is not a lambda")
                }
            } else {
                (usage, false)
            }
        }

        ExprKind::Merge { ref builder, ref value } => {
            let (builder_usage, builder_ret) = compute_appender_usage(builder, appender_sym);
            let (value_usage, value_ret) = compute_appender_usage(value, appender_sym);
            assert!(!value_ret);  // Assuming that one cannot merge an appender into a builder...

            let mut usage = builder_usage.compose_seq(value_usage);
            if builder_ret {  // We're merging into the appender in question.
                usage = usage.compose_seq(AppenderUsage::Once)
            }
            (usage, builder_ret)
        }

        ExprKind::Res { ref builder } => {
            let (builder_usage, builder_ret) = compute_appender_usage(builder, appender_sym);
            if builder_ret {
                // This probably shouldn't happen (taking result of appender)...
                (AppenderUsage::Unsupported, false)
            } else {
                (builder_usage, false)
            }
        }

        _ => (AppenderUsage::Unsupported, false)
    }
}
