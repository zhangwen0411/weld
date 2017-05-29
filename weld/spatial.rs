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
    pub extra_host_init: CodeBuilder,
    pub extra_accel_init: CodeBuilder,
}

impl GlobalCtx {
    pub fn new(weld_ast: &TypedExpr) -> GlobalCtx {
        GlobalCtx {
            code_builders: Vec::new(),
            sym_gen: SymbolGenerator::from_expression(weld_ast),

            extra_host_init: CodeBuilder::new(), // Before Accel block
            extra_accel_init: CodeBuilder::new(), // At the beginning of Accel block
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

    pub fn add_dram(&mut self, sym: &Symbol, elem_ty_scala: &String, len: &Symbol)
    {
        self.extra_host_init.add(format!("val {} = DRAM[{}]({})",
              gen_sym(&sym), elem_ty_scala, gen_sym(&len)));
    }
}

macro_rules! add_code {
    ( $glob_ctx:expr, $($arg:tt)* ) => ({
        $glob_ctx.add_code(format!($($arg)*))
    })
}

#[derive(Clone)]
struct VecMergerState<'a> {
    sram_sym: &'a Symbol,
    range_start_sym: &'a Symbol,
    range_size_sym: &'a Symbol,
}

#[derive(Clone)]
struct VectorInfo {
    len: Symbol, // Vector length.
    len_bound: Symbol, // Static upper bound on vector length.
}

#[derive(Clone, PartialEq)]
enum AppenderState<'a> {
    Fresh, // Hasn't been used
    Map { sram_sym: &'a Symbol, ii_sym: &'a Symbol }, // Is being used in a map operation

    // Is being used in a filter operation.
    // Here "filter" just means "at most one append for every element"; the value appended
    // can be different from the original value.
    Filter { fifo_sym: &'a Symbol },

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
struct LocalCtx<'a> {
    vectors: HashMap<Symbol, VectorInfo>,
    pub vecmergers: HashMap<Symbol, VecMergerState<'a>>,
    pub appenders: HashMap<Symbol, AppenderState<'a>>,
    pub structs: HashMap<Symbol, Box<Vec<Symbol>>>, // symbol for struct value => components
}

impl<'a> LocalCtx<'a> {
    pub fn new() -> LocalCtx<'a> {
        LocalCtx {
            vectors: HashMap::new(),
            vecmergers: HashMap::new(),
            appenders: HashMap::new(),
            structs: HashMap::new(),
        }
    }

    pub fn register_vec(&mut self, vec: &Symbol, len: &Symbol, len_bound: &Symbol) {
        let vec_info = VectorInfo { len: len.clone(), len_bound: len_bound.clone() };
        self.vectors.insert(vec.clone(), vec_info);
    }

    /// Panics if vec hasn't been registered.
    pub fn get_vec_info(&self, vec: &Symbol) -> VectorInfo {
        self.vectors.get(vec).unwrap().clone()
    }

    /// Panics if vec hasn't been registered.
    pub fn duplicate_vec_info(&mut self, vec: &Symbol, new_vec: &Symbol) -> VectorInfo {
        let vec_info = self.vectors.get(vec).unwrap().clone();
        self.vectors.insert(new_vec.clone(), vec_info.clone());
        vec_info
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
        let (output_prologue, output_body, output_epilogue) = gen_spatial_output_setup(&body.ty)?;
        add_code!(glob_ctx, "{}", output_prologue);

        glob_ctx.enter_builder_scope();
        let res_sym = gen_expr(body, &mut glob_ctx, &mut local_ctx)?;
        let accel_body = glob_ctx.exit_builder_scope();

        let extra_host_init_code = glob_ctx.extra_host_init.result().to_string();
        add_code!(glob_ctx, "{}", extra_host_init_code);

        // Generate Spatial code (Accel block).
        add_code!(glob_ctx, "Accel {{");
        let extra_accel_init_code = glob_ctx.extra_accel_init.result().to_string();
        add_code!(glob_ctx, "{}", extra_accel_init_code);
        add_code!(glob_ctx, "{}", accel_body);

        add_code!(glob_ctx, "{}", output_body(&res_sym, &local_ctx));
        add_code!(glob_ctx, "}}"); // Accel

        // Return value.
        add_code!(glob_ctx, "{}", output_epilogue(&res_sym, &local_ctx));
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

            let veclen_sym = glob_ctx.add_variable();
            local_ctx.register_vec(&param.name, &veclen_sym, &veclen_sym);
            let veclen_sym_name = gen_sym(&veclen_sym);
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

type CodeF = Box<Fn(&Symbol, &LocalCtx) -> String>;

/// Returns (prologue, body, epilogue).
fn gen_spatial_output_setup(ty: &Type) -> WeldResult<(String, CodeF, CodeF)> {
    match *ty {
        Type::Scalar(scalar_kind) => {
            let prologue = format!("val out = ArgOut[{}]",
                                   gen_scalar_type_from_kind(scalar_kind));
            let body = Box::new(|res: &Symbol, _: &LocalCtx|
                                format!("out := {}", gen_sym(&res)));
            let epilogue = Box::new(|_: &Symbol, _: &LocalCtx|
                                    String::from("getArg(out)"));
            Ok((prologue, body, epilogue))
        }

        Type::Vector(ref boxed_ty) => {
            let err_msg = format!("Not supported: nested type {}", print_type(ty));
            gen_scalar_type(boxed_ty, err_msg)?;

            let prologue = String::from("val len = ArgOut[Index]");
            let body = Box::new(|res: &Symbol, local_ctx: &LocalCtx|
                                format!("len := {}", gen_sym(&local_ctx.get_vec_info(&res).len)));
            let epilogue = Box::new(|res: &Symbol, _: &LocalCtx|
                                    format!("pack(getMem({}), getArg(len))", gen_sym(&res)));
            Ok((prologue, body, epilogue))
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
                    // FIXME(zhangwen): I don't seem to need "Sequential" inside if body.  Why?
                    add_code!(glob_ctx, "\
                        if ({cond}) {{
                            {on_true_code}
                        }} else {{
                            {on_false_code}
                        }}",

                        cond = gen_sym(&cond_sym),
                        on_true_code = on_true_code,
                        on_false_code = on_false_code);

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

                use ast::BuilderKind::*;
                match *builder_kind {
                    Merger(ref elem_ty, binop) =>
                        gen_merger_loop(&data_res, elem_ty, body, &builder_sym, binop,
                                        b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                    VecMerger(ref elem_ty, binop) =>
                        gen_vecmerger_loop(&data_res, data_ty, elem_ty, body, &builder_sym, binop,
                                           b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                    Appender(ref elem_ty) => {
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

                        use self::AppenderUsage::*;
                        match usage {
                            Once => gen_map_loop(&data_res, elem_ty, body, &builder_sym,
                                                 b_sym, i_sym, e_sym, glob_ctx, local_ctx),
                            AtMostOnce =>
                                gen_filter_loop(&data_res, elem_ty, body, &builder_sym,
                                                b_sym, i_sym, e_sym, glob_ctx, local_ctx),
                            Unused | Unsupported =>
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
                use ast::BuilderKind::*;
                match *kind {
                    Merger(ref elem_ty, _) => gen_new_merger(elem_ty, glob_ctx, None),

                    VecMerger(ref elem_ty, _) => {
                        let vec_sym = gen_expr(builder_expr.as_ref().unwrap(),
                                               glob_ctx, local_ctx)?;
                        gen_new_vecmerger(&vec_sym, elem_ty, glob_ctx, local_ctx)
                    }

                    Appender(_) => gen_new_appender(glob_ctx, local_ctx),

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
                use ast::BuilderKind::*;
                match *kind {
                    Merger(_, binop) => {
                        let res_sym = glob_ctx.add_variable();
                        add_code!(glob_ctx, "val {new_val} = {old_val} {op} {value}",
                                  new_val = gen_sym(&res_sym),
                                  old_val = gen_sym(&builder_sym),
                                  op = binop,
                                  value = gen_sym(&value_res));
                        Ok(res_sym)
                    }

                    VecMerger(_, binop) =>
                        if let Some(vecmerger_state) = local_ctx.vecmergers.get(&builder_sym) {
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

                                offset = offset_sym_name,
                                index = gen_sym(index_sym),
                                start = gen_sym(&vecmerger_state.range_start_sym),
                                range_size = gen_sym(&vecmerger_state.range_size_sym),
                                sram = gen_sym(&vecmerger_state.sram_sym),
                                binop = binop,
                                value = gen_sym(merge_val_sym)
                            );
                            Ok(builder_sym)
                        } else {
                            weld_err!("Not supported: vecmerger merge {}", print_expr(expr))
                        },

                    Appender(_) => {
                        use self::AppenderState::*;
                        let appender_state = local_ctx.appenders.get(&builder_sym).unwrap();
                        match *appender_state {
                            Map { ref sram_sym, ref ii_sym } => {
                                add_code!(glob_ctx, "{sram}({ii}) = {value}",
                                          sram = gen_sym(&sram_sym),
                                          ii = gen_sym(&ii_sym),
                                          value = gen_sym(&value_res));
                                Ok(builder_sym)
                            }

                            Filter { ref fifo_sym } => {
                                add_code!(glob_ctx, "{fifo}.enq({value})",
                                          fifo = gen_sym(&fifo_sym),
                                          value = gen_sym(&value_res));
                                Ok(builder_sym)
                            }

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
                use ast::BuilderKind::*;
                match *kind {
                    Merger(_, _) | VecMerger(_, _) | Appender(_) => Ok(builder_sym),
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

fn gen_vecmerger_loop(data_sym: &Symbol, data_ty: &Type, dst_ty: &Type, body: &TypedExpr,
                      dst_sym: &Symbol, binop: BinOpKind,
                      b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                      glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    const BLK_SIZE_DST: i32 = 16; // TODO(zhangwen): tunable parameter.
    const BLK_SIZE_SRC: i32 = 32; // TODO(zhangwen): tunable parameter.
    const PAR: i32 = 4;
    let dst_veclen_sym = &local_ctx.get_vec_info(&dst_sym).len;

    // Merge into blocks of vecmerger, one by one.
    let dst_ty_scala = gen_scalar_type(dst_ty, format!(
            "Not supported vecmerger elem type: {}", print_type(dst_ty)))?;
    let data_ty_scala = gen_scalar_type(data_ty, format!(
            "Not supported data vec elem type: {}", print_type(data_ty)))?;
    let dst_sram_sym = glob_ctx.add_variable();
    let range_start_sym = glob_ctx.add_variable();
    let dst_block_size_sym = glob_ctx.add_variable();
    let data_len_sym = &local_ctx.get_vec_info(&data_sym).len;
    let local_dst_sram_sym = glob_ctx.add_variable();

    // Generate body code.
    let mut sub_local_ctx = local_ctx.clone();
    sub_local_ctx.vecmergers.insert(b_sym.clone(),
        VecMergerState { sram_sym: &local_dst_sram_sym, range_start_sym: &range_start_sym,
                         range_size_sym: &dst_block_size_sym });
    glob_ctx.enter_builder_scope();
    let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
    assert_eq!(body_res, *b_sym); // Body should return builder derived from `b`.
    let body_code = glob_ctx.exit_builder_scope();

    /*
     * Here's an illustration for par=4, from the perspective of par_id=2.
     *
     *                                working on this block
     *           /---- round_blk ---\            v
     *           ---------------------------------------------------------
     * data_dram |    |    |    |    &    |    |xxxx|    &    |    |  ...
     *           ---------------------------------------------------------
     *                     ^         ^         ^
     *                par_offset     i    i+par_offset
     *
     * The blocks in one "round" are processed in parallel before we move on to the next "round".
     */

    // FIXME(zhangwen): doesn't work for, say, multiplication.
    add_code!(glob_ctx, "\
        Pipe({dst_len} by {dst_blk}) {{ {rs} =>
            val {dst_sram} = SRAM[{ty}]({dst_blk})
            val {dst_block_size} = min({dst_len} - {rs}, {dst_blk}.to[Index])
            {dst_sram} load {dst_dram}({rs}::{rs}+{dst_block_size})

            val round_blk = {data_blk} * {par}
            MemFold({dst_sram})({par} by 1 par {par}) {{ par_id =>
                val par_offset = par_id * {data_blk}
                val {local_dst_sram} = SRAM[{ty}]({dst_blk})
                Pipe({dst_blk} by 1) {{ ii => {local_dst_sram}(ii) = 0 }}

                Pipe({data_len} by round_blk) {{ i =>
                    val base = par_offset + i
                    val data_sram = SRAM[{data_ty}]({data_blk})
                    val data_block_size =
                        min(max({data_len} - base, 0.to[Index]), {data_blk}.to[Index])
                    data_sram load {data_dram}(base::base+data_block_size)

                    Pipe(data_block_size by 1) {{ ii =>
                        // TODO(zhangwen): Spatial indices are 32-bit.
                        val {i_sym} = (base + ii).to[Long]
                        val {e_sym} = data_sram(ii)

                        Sequential {{
                            {body_code}
                        }}  // Sequential
                    }}  // Pipe
                }}  // Pipe

                {local_dst_sram}
            }} {{ _{binop}_ }}  // MemFold

            {dst_dram}({rs}::{rs}+{dst_block_size}) store {dst_sram}
        }}  // Pipe",

        dst_len = gen_sym(&dst_veclen_sym),
        dst_blk = BLK_SIZE_DST,
        par = PAR,
        rs = gen_sym(&range_start_sym),
        dst_block_size = gen_sym(&dst_block_size_sym),
        dst_sram = gen_sym(&dst_sram_sym),
        dst_dram = gen_sym(&dst_sym),
        ty = dst_ty_scala,
        data_ty = data_ty_scala,
        data_len = gen_sym(&data_len_sym),
        data_blk = BLK_SIZE_SRC,
        data_dram = gen_sym(&data_sym),
        local_dst_sram = gen_sym(&local_dst_sram_sym),
        i_sym = gen_sym(i_sym),
        e_sym = gen_sym(e_sym),
        body_code = body_code,
        binop = binop);

    Ok(dst_sym.clone())
}

fn gen_merger_loop(data_sym: &Symbol, elem_ty: &Type, body: &TypedExpr,
                   merger_sym: &Symbol, binop: BinOpKind,
                   b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                   glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    const BLK_SIZE: i32 = 16; // TODO(zhangwen): tunable parameter.
    let veclen_sym = &local_ctx.get_vec_info(&data_sym).len;
    let reduce_res = glob_ctx.add_variable();
    let merger_type_scala = gen_scalar_type(elem_ty, format!(
            "Not supported merger elem type: {}", print_type(elem_ty)))?;

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
        val {reduce_res} = Reduce(Reg[{ty}])({veclen} by {blk}){{ i =>
            val block = SRAM[{ty}]({blk})
            val block_len = min({veclen} - i, {blk}.to[Index])
            block load {dram}(i::i+block_len)
            Reduce(Reg[{ty}])(block_len by 1){{ ii =>
                val {i_sym} = (i + ii).to[Long]
                val {e_sym} = block(ii)

                {body_code}
                {body_res}
            }} {{ _{binop}_ }}  // Reduce
        }} {{ _{binop}_ }} {binop} {init_value}  // Reduce",

        veclen = gen_sym(&veclen_sym),
        blk = BLK_SIZE,
        reduce_res = gen_sym(&reduce_res),
        ty = merger_type_scala,
        dram = gen_sym(data_sym),
        i_sym = gen_sym(i_sym),
        e_sym = gen_sym(e_sym),
        body_code = body_code,
        body_res = gen_sym(&body_res),
        binop = binop,
        init_value = gen_sym(&merger_sym));

    Ok(reduce_res)
}

fn gen_map_loop(data_sym: &Symbol, elem_ty: &Type, body: &TypedExpr,
                appender_sym: &Symbol, b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported appender type: {}",
                                                           print_type(elem_ty)))?;
    // The result of a map operation has the same length as the data.
    let vec_info = local_ctx.duplicate_vec_info(&data_sym, &appender_sym);
    glob_ctx.add_dram(&appender_sym, &elem_type_scala, &vec_info.len_bound);

    let sram_dst_sym = glob_ctx.add_variable();
    let ii_sym = glob_ctx.add_variable();

    // Generate body code.
    let mut sub_local_ctx = local_ctx.clone();
    sub_local_ctx.appenders.insert(
        b_sym.clone(), AppenderState::Map { sram_sym: &sram_dst_sym, ii_sym: &ii_sym });
    glob_ctx.enter_builder_scope();
    let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
    assert_eq!(body_res, *b_sym); // Body should return builder derived from `b`.
    let body_code = glob_ctx.exit_builder_scope();

    const BLK_SIZE: i32 = 16;
    add_code!(glob_ctx, "\
        Pipe({veclen} by {blk}) {{ i =>
            val sram_data = SRAM[{ty}]({blk})
            val {sram_dst} = SRAM[{ty}]({blk})
            val block_size = min({veclen} - i, {blk}.to[Index])
            sram_data load {data}(i::i+block_size)
            Pipe(block_size by 1) {{ {ii} =>
                val {i_sym} = (i + {ii}).to[Long]
                val {e_sym} = sram_data({ii})

                Sequential {{
                    {body_code}
                }}
            }}
            {dst}(i::i+block_size) store {sram_dst}
        }}",

        veclen = gen_sym(&vec_info.len),
        blk = BLK_SIZE,
        ty = elem_type_scala,
        sram_dst = gen_sym(&sram_dst_sym),
        data = gen_sym(&data_sym),
        ii = gen_sym(&ii_sym),
        i_sym = gen_sym(&i_sym),
        e_sym = gen_sym(&e_sym),
        body_code = body_code,
        dst = gen_sym(&appender_sym));

    Ok(appender_sym.clone())
}

fn gen_filter_loop(data_sym: &Symbol, elem_ty: &Type, body: &TypedExpr,
                   appender_sym: &Symbol, b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                   glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported appender type: {}",
                                                           print_type(elem_ty)))?;
    let data_vec_info = local_ctx.get_vec_info(&data_sym);
    let data_len = &data_vec_info.len;
    glob_ctx.add_dram(&appender_sym, &elem_type_scala, &data_vec_info.len_bound);
    let result_veclen_sym = glob_ctx.add_variable();
    glob_ctx.extra_accel_init.add(format!("val {} = Reg[Index](0.to[Index])",
                                           gen_sym(&result_veclen_sym)));
    local_ctx.register_vec(&appender_sym, &result_veclen_sym, &data_vec_info.len_bound);

    const BLK_SIZE: usize = 16;
    const PAR: usize = 4;

    // Manually unroll for parallelism...
    // FIXME(zhangwen): can't use design space exploration on PAR this way...
    add_code!(glob_ctx, "\
        val round_blk = {blk} * {par}
        Sequential({data_len} by round_blk) {{ i =>",
        blk = BLK_SIZE, par = PAR, data_len = gen_sym(&data_len));

    // Make `PAR` local FIFOs.
    let mut fifo_syms = Vec::new();
    for _ in 0..PAR {
        let fifo_sym = glob_ctx.add_variable();
        add_code!(glob_ctx, "val {fifo} = FIFO[{ty}]({blk})",
                  fifo = gen_sym(&fifo_sym), ty = elem_type_scala, blk = BLK_SIZE);
        fifo_syms.push(fifo_sym);
    }
    let fifo_syms = fifo_syms;

    // Evaluate condition on blocks in parallel.
    add_code!(glob_ctx, "Parallel {{");
    for (par_id, fifo_sym) in fifo_syms.iter().enumerate() {
        // Generate body code.
        // FIXME(zhangwen): shouldn't need to generate body code `PAR` times...
        let mut sub_local_ctx = local_ctx.clone();
        sub_local_ctx.appenders.insert(
            b_sym.clone(), AppenderState::Filter { fifo_sym: &fifo_sym });
        glob_ctx.enter_builder_scope();
        let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
        assert_eq!(body_res, *b_sym); // Body should return builder derived from `b`.
        let body_code = glob_ctx.exit_builder_scope();

        add_code!(glob_ctx, "\
        {{  // #{par_id}
            val base = (i + {par_id}*{blk}).to[Index]
            val block_len = min(max({data_len} - base, 0.to[Index]), {blk}.to[Index])
            val sram_data = SRAM[{ty}]({blk})
            sram_data load {data}(base::base+block_len)

            Pipe(block_len by 1) {{ ii =>
                val {i_sym} = (base + ii).to[Long]
                val {e_sym} = sram_data(ii)

                Sequential {{
                    {body_code}
                }}  // Sequential
            }}  // Pipe
        }}  // #{par_id}
        ",

        par_id = par_id,
        blk = BLK_SIZE,
        data_len = gen_sym(&data_len),
        ty = elem_type_scala,
        data = gen_sym(&data_sym),
        i_sym = gen_sym(&i_sym),
        e_sym = gen_sym(&e_sym),
        body_code = body_code);
    }
    add_code!(glob_ctx, "}}  // Parallel");

    // Compute prefix sums.
    // TODO(zhangwen): smarter prefix sum.
    let mut count_syms = Vec::new();
    for par_id in 0usize..PAR {
        let count_sym = glob_ctx.add_variable();
        let sum = fifo_syms.iter().take(par_id + 1) // Construct the sum expression.
                           .map(|s| gen_sym(&s) + ".numel")
                           .collect::<Vec<_>>().join("+");
        add_code!(glob_ctx, "val {} = {}", gen_sym(&count_sym), sum);
        count_syms.push(count_sym);
    }
    let count_syms = count_syms;

    // Stuff the elements into the destination DRAM.
    add_code!(glob_ctx, "Parallel {{");
    for (fifo_sym, count_sym) in fifo_syms.iter().zip(count_syms.iter()) {
        add_code!(glob_ctx, "\
                  {appender}({result_len}+{end}-{fifo}.numel::{result_len}+{end}) store {fifo}",
                  appender = gen_sym(&appender_sym),
                  result_len = gen_sym(&result_veclen_sym),
                  end = gen_sym(&count_sym),
                  fifo = gen_sym(&fifo_sym));
    }
    add_code!(glob_ctx, "}}  // Parallel");

    // Update number of elements so far (i.e., output length).
    add_code!(glob_ctx, "{result_len} := {result_len} + {round_count}",
              result_len = gen_sym(&result_veclen_sym),
              round_count = gen_sym(&count_syms.last().unwrap()));

    add_code!(glob_ctx, "}}  // Sequential");

    Ok(appender_sym.clone())
}

fn gen_new_vecmerger(vec_sym: &Symbol, elem_ty: &Type, glob_ctx: &mut GlobalCtx,
                     local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported vecmerger type: {}",
                                                           print_type(elem_ty)))?;
    let vecmerger_sym = glob_ctx.add_variable();
    let vec_info = local_ctx.duplicate_vec_info(&vec_sym, &vecmerger_sym);
    glob_ctx.add_dram(&vecmerger_sym, &elem_type_scala, &vec_info.len_bound);

    // Copy from `vec_sym` to `vecmerger_sym`. Could do this lazily, but probably not worth it.
    const BLK_SIZE: i32 = 16; // TODO(zhangwen): tunable parameter.
    add_code!(glob_ctx, "\
        Pipe({veclen} by {blk}) {{ i =>
            val ss = SRAM[{ty}]({blk})
            ss load {vec}(i::i+{blk})
            {vecmerger}(i::i+{blk}) store ss
        }} // Pipe",

        veclen = gen_sym(&vec_info.len),
        blk = BLK_SIZE,
        ty = elem_type_scala,
        vec = gen_sym(&vec_sym),
        vecmerger = gen_sym(&vecmerger_sym));

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
        add_code!(glob_ctx, "val {} = mux({}, {}, {})",
                  gen_sym(&res_sym), gen_sym(&cond_sym),
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
            use ast::BuilderKind::*;
            match *kind {
                Appender(_) | DictMerger(_, _, _) | VecMerger(_, _) => true,
                Merger(_, _) => false,
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
                    if func_usage != AppenderUsage::Unused {
                        (AppenderUsage::Unsupported, true)
                    } else {
                        (usage, true)
                    }
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
