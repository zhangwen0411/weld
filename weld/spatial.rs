//! Emits Spatial code for Weld AST.

use std::collections::{HashMap, HashSet};

use super::ast::*;
use super::code_builder::CodeBuilder;
use super::error::*;
use super::pretty_print::*;
use super::util::SymbolGenerator;

#[derive(Clone)]
struct VectorInfo {
    len: Symbol, // Vector length.
    len_bound: Symbol, // Static upper bound on vector length; used as Spatial DRAM size.
    elem_ty: Type, // Element type; currently has to be scalar.
}

struct GlobalCtx {
    sym_gen: SymbolGenerator,

    // A stack of CodeBuilders in use.  The last element is the "active" one; calling `add_code` on
    // a GlobalCtx will add code to the active CodeBuilder.
    code_builders: Vec<CodeBuilder>,

    vectors: HashMap<Symbol, VectorInfo>, // vector symbol => vector information
    pub structs: HashMap<Symbol, Vec<Symbol>>, // symbol for struct value => components

    extra_host_init: CodeBuilder,  // Extra init code to be added before Accel block
    extra_accel_init: CodeBuilder, // Extra init code to be added at the start of Accel block
}

impl GlobalCtx {
    pub fn new(weld_ast: &TypedExpr) -> GlobalCtx {
        GlobalCtx {
            code_builders: Vec::new(),
            sym_gen: SymbolGenerator::from_expression(weld_ast),

            vectors: HashMap::new(),
            structs: HashMap::new(),

            extra_host_init: CodeBuilder::new(), // Before Accel block
            extra_accel_init: CodeBuilder::new(), // At the beginning of Accel block
        }
    }

    /// Pushes a fresh code builder as the active one.
    pub fn enter_builder_scope(&mut self) {
        self.code_builders.push(CodeBuilder::new());
    }

    /// Pops the current code builder and returns its contents.
    pub fn exit_builder_scope(&mut self) -> String {
        self.code_builders.pop().unwrap().result().to_string()
    }

    /// Returns a globally unique Symbol.
    pub fn add_variable(&mut self) -> Symbol {
        self.sym_gen.new_symbol("tmp")
    }

    /// Returns a vector of `count` globally unique Symbols.
    pub fn add_variables(&mut self, count: usize) -> Vec<Symbol> {
        (0..count).map(|_| self.add_variable()).collect()
    }

    /// Adds code to the active CodeBuilder.
    pub fn add_code<S>(&mut self, code: S) where S: AsRef<str> {
        self.code_builders.last_mut().unwrap().add(code);
    }

    /// Registers a vector with the global context.
    pub fn add_vec(&mut self, vec: &Symbol, len: &Symbol, len_bound: &Symbol, elem_ty: &Type) {
        self.vectors.insert(vec.clone(), VectorInfo {
            len: len.clone(), len_bound: len_bound.clone(), elem_ty: elem_ty.clone() });
    }

    /// Creates a vector (DRAM) with the same element type, length, and length bound as an
    /// existing vector.  Elem type is also the same unless specified.  Panics if existing vector
    /// doesn't exist.  Returns length symbol.
    pub fn duplicate_vec(&mut self, old_vec: &Symbol, new_vec: &Symbol,
                         o_elem_ty: Option<Type>) -> Symbol {
        let mut vec_info = self.vectors.get(old_vec).unwrap().clone();
        if let Some(elem_ty) = o_elem_ty {
            vec_info.elem_ty = elem_ty;
        }
        let vec_info = vec_info;

        let len_sym = vec_info.len.clone();
        let elem_ty_scala = gen_scalar_type(&vec_info.elem_ty, format!("Boo")).unwrap();
        self.extra_host_init.add(format!("val {} = DRAM[{}]({})",
              gen_sym(&new_vec), elem_ty_scala, gen_sym(&vec_info.len_bound)));
        self.vectors.insert(new_vec.clone(), vec_info);
        len_sym
    }

    /// Same as `duplicate_vec`, except that the new vector has its own length variable,
    /// initialized to zero.  Returns length Symbol.
    pub fn duplicate_vec_empty(&mut self, old_vec: &Symbol, new_vec: &Symbol, elem_ty: Type)
                               -> Symbol {
        let elem_ty_scala = gen_scalar_type(&elem_ty, format!("Boo")).unwrap();

        let new_len_sym = self.add_variable();
        let new_vec_info = {
            let old_vec_info = self.vectors.get(old_vec).unwrap();
            VectorInfo {
                len: new_len_sym.clone(), len_bound: old_vec_info.len_bound.clone(),
                elem_ty: elem_ty,
            }
        };
        self.extra_accel_init.add(
            format!("val {} = Reg[Index](0.to[Index])", gen_sym(&new_len_sym)));
        self.extra_host_init.add(format!("val {} = DRAM[{}]({})",
              gen_sym(&new_vec), elem_ty_scala, gen_sym(&new_vec_info.len_bound)));
        self.vectors.insert(new_vec.clone(), new_vec_info);
        new_len_sym
    }

    /// Panics if vec hasn't been added.
    pub fn get_vec_info(&self, vec: &Symbol) -> VectorInfo {
        self.vectors.get(vec).unwrap().clone()
    }
}

/// Helper macro for adding code to global context.
macro_rules! add_code {
    ( $glob_ctx:expr, $($arg:tt)* ) => ({
        $glob_ctx.add_code(format!($($arg)*))
    })
}

/// Helps generate code for merging into a vecmerger.
#[derive(Clone)]
struct VecMergerState<'a> {
    sram_sym: &'a Symbol, // SRAM to merge into (a block from destination vector)

    // Starting index and extent of block.  A `merge(b, {i, e})` where `i` is outside this range
    // should be ignored because it's not represented in the block.
    range_start_sym: &'a Symbol,
    range_size_sym: &'a Symbol,
}

/// If an appender has been used / how an appender is being used.
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

/// Characterizes the usage pattern for an appender in a piece of code.
#[derive(Debug, PartialEq)]
enum AppenderUsage {
    Unused, // The appender is not used.
    Once, // The appdner is used exactly once.
    AtMostOnce, // The appender is used <= once.
    Unsupported, // Any other usage pattern.
}

impl AppenderUsage {
    /// Computes appender usage for one piece of code followed by another (`self` then `other`).
    fn compose_seq(self, other: AppenderUsage) -> AppenderUsage {
        use self::AppenderUsage::*;
        match (self, other) {
            (u @ _, Unused) | (Unused, u @ _) => u,
            _ => Unsupported,
        }
    }

    /// Computes appender usage for two pieces of code, exactly one of which is executed.
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
    pub vecmergers: HashMap<Symbol, VecMergerState<'a>>,
    pub appenders: HashMap<Symbol, AppenderState<'a>>,

    // Maps a symbol to the symbol for which it's an alias.
    // For example, within `let foo = bar; ...`, `foo` is mapped to `bar`.
    aliases: HashMap<Symbol, Symbol>,
}

impl<'a> LocalCtx<'a> {
    pub fn new() -> LocalCtx<'a> {
        LocalCtx {
            vecmergers: HashMap::new(),
            appenders: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    /// Adds `alias` as an alias of `sym`.
    pub fn add_alias(&mut self, alias: Symbol, sym: &Symbol) {
        // If `sym` has a "parent", the parent has to be a root.
        let sym = if let Some(ref parent) = self.aliases.get(sym) { parent } else { sym }.clone();
        self.aliases.insert(alias, sym);
    }

    /// Returns the symbol for which `alias` is an alias.
    pub fn resolve_alias(&self, alias: &Symbol) -> Symbol {
        (if let Some(sym) = self.aliases.get(alias) { sym } else { alias }).clone()
    }

    /// Returns true if the two symbols are aliases of each other.
    pub fn are_alias(&self, sym1: &Symbol, sym2: &Symbol) -> bool {
        self.resolve_alias(sym1) == self.resolve_alias(sym2)
    }
}

/// Returns Spatial code, in text, generated from Weld AST, or error if a Weld usage pattern is not
/// supported.  The Weld AST has to be a lambda.
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
            gen_spatial_input_param_setup(param, scala_param_name, &mut glob_ctx)?;
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

        let output_body = output_body(&res_sym, &glob_ctx);
        add_code!(glob_ctx, "{}", output_body);
        add_code!(glob_ctx, "}}"); // Accel

        // Return value.
        let output_epilogue = output_epilogue(&res_sym, &glob_ctx);
        add_code!(glob_ctx, "{}", output_epilogue);
        add_code!(glob_ctx, "}}"); // def spatialProg

        Ok(glob_ctx.exit_builder_scope())
    } else {
        weld_err!("Expression passed to ast_to_spatial was not a Lambda")
    }
}

/// Returns the Scala type of a parameter representing a Weld value of type `ty`.
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

/// Returns setup code for setting up Spatial parameters.
fn gen_spatial_input_param_setup(param: &Parameter<Type>,
                                 scala_param_name: String,
                                 glob_ctx: &mut GlobalCtx)
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
            glob_ctx.add_vec(&param.name, &veclen_sym, &veclen_sym, boxed_ty);
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

/// Takes (result symbol, global context) and returns code as string.
type CodeF = Box<Fn(&Symbol, &GlobalCtx) -> String>;

/// Returns (prologue, body, epilogue).
fn gen_spatial_output_setup(ty: &Type) -> WeldResult<(String, CodeF, CodeF)> {
    match *ty {
        Type::Scalar(scalar_kind) => {
            let prologue = format!("val out = ArgOut[{}]",
                                   gen_scalar_type_from_kind(scalar_kind));
            let body = Box::new(|res: &Symbol, _: &GlobalCtx|
                                format!("out := {}", gen_sym(&res)));
            let epilogue = Box::new(|_: &Symbol, _: &GlobalCtx|
                                    String::from("getArg(out)"));
            Ok((prologue, body, epilogue))
        }

        Type::Vector(ref boxed_ty) => {
            let err_msg = format!("Not supported: nested type {}", print_type(ty));
            gen_scalar_type(boxed_ty, err_msg)?;

            let prologue = String::from("val len = ArgOut[Index]");
            let body = Box::new(|res: &Symbol, glob_ctx: &GlobalCtx|
                                format!("len := {}", gen_sym(&glob_ctx.get_vec_info(&res).len)));
            let epilogue = Box::new(|res: &Symbol, _: &GlobalCtx|
                                    format!("pack(getMem({}), getArg(len))", gen_sym(&res)));
            Ok((prologue, body, epilogue))
        }

        _ => weld_err!("Not supported: result type {}", print_type(ty))
    }
}

/// Generates Scala type for Weld scalar kind.
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

/// Generates Scala type for Weld scalar type.
/// Returns WeldError with error message if `ty` is not a scalar type.
fn gen_scalar_type(ty: &Type, err_msg: String) -> WeldResult<String> {
    match *ty {
        Type::Scalar(scalar_kind) => Ok(gen_scalar_type_from_kind(scalar_kind)),
        _ => Err(WeldError::new(err_msg)),
    }
}

/// Emits Spatial code for `expr` to the current CodeBuilder in `glob_ctx` and returns a Symbol for
/// the result of `expr`.
///
/// The meaning of the returned Symbol depends on the type of `expr`:
/// - If `expr` is a scalar or a merger, the Symbol names a Spatial value for the result of
///   `expr`.  It can be a simple value (as in `val tmp_10 = 5`) or a Reg; this distinction
///   shouldn't matter syntactically as the Spatial compiler adds register reads implicitly.
/// - If `expr` is a vecmerger or an appender, the Symbol names the Spatial DRAM backing the
///   builder.  The Symbol has been registered as a vector in `glob_ctx`.  Since the Spatial DRAM
///   is declared outside the Accel block, it is accessible globally.
/// - If `expr` is a struct, the Symbol uniquely identifies the struct and has no meaning in
///   Spatial.  The Symbol has been registered as a struct in `glob_ctx`.
fn gen_expr(expr: &TypedExpr, glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx)
            -> WeldResult<Symbol> {
    match expr.kind {
        ExprKind::Ident(ref sym) => Ok(local_ctx.resolve_alias(sym)),

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

        ExprKind::Let { ref name, ref value, ref body } => {
            let value_res_sym = gen_expr(value, glob_ctx, local_ctx)?;
            let mut sub_local_ctx = local_ctx.clone();
            sub_local_ctx.add_alias(name.clone(), &value_res_sym);
            gen_expr(body, glob_ctx, &mut sub_local_ctx)
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

            if is_spatial_first_class(&expr.ty) {
                // Mux the two values!
                add_code!(glob_ctx, "{}", on_true_code);
                add_code!(glob_ctx, "{}", on_false_code);

                let res_sym = if on_true_sym == on_false_sym {
                    on_true_sym.clone()
                } else {
                    let res_sym = glob_ctx.add_variable();
                    add_code!(glob_ctx, "val {} = mux({}, {}, {})",
                              gen_sym(&res_sym), gen_sym(&cond_sym),
                              gen_sym(&on_true_sym), gen_sym(&on_false_sym));
                    res_sym
                };
                Ok(res_sym)
            } else {
                if on_true_sym != on_false_sym {
                    // E.g., a conditional over two different vectors is not allowed.
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
            }
        }

        ExprKind::For { ref iters, ref builder, ref func } => {
            // iters
            if iters.is_empty() {
                weld_err!("For without iters: {}", print_expr(expr))?
            }

            // Evaluate the vectors to iterate over.
            let data_syms = iters.iter().map(|iter| {
                if iter.start.is_some() || iter.end.is_some() || iter.stride.is_some() {
                    weld_err!("Not supported: iter with start, end, and/or stride")
                } else {
                    use ast::Type::*;
                    match iter.data.ty {
                        Vector(ref boxed_ty) => 
                            if let Scalar(_) = **boxed_ty {
                                gen_expr(&iter.data, glob_ctx, local_ctx)
                            } else {
                                weld_err!("iter not scalar : {}", print_expr(&iter.data))
                            },

                        _ => weld_err!("iter a vector: {}", print_expr(&iter.data))
                    }
                }
            }).collect::<WeldResult<Vec<Symbol>>>()?;

            // builder
            let builder_sym = gen_expr(builder, glob_ctx, local_ctx)?;
            let builder_kind = match builder.ty {
                Type::Builder(ref kind, _) => kind,
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
                        gen_merger_loop(&data_syms, elem_ty, body, &builder_sym, binop,
                                        b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                    VecMerger(ref elem_ty, binop) =>
                        gen_vecmerger_loop(&data_syms, elem_ty, body, &builder_sym,
                                           binop, b_sym, i_sym, e_sym, glob_ctx, local_ctx),

                    Appender(ref elem_ty) => {
                        // Prohibit the appender from being used again.
                        let old_state = local_ctx.appenders.insert(builder_sym.clone(),
                                                                   AppenderState::Dead).unwrap();
                        // Make sure that the appender was fresh.
                        if old_state != AppenderState::Fresh {
                            weld_err!("Appender {} reused: {}", builder_sym, print_expr(expr))?;
                        }

                        let (usage, ret) = compute_appender_usage(body, b_sym);

                        use self::AppenderUsage::*;
                        if usage != Unsupported && !ret {
                            weld_err!("For loop body doesn't return builder: {}",
                                      print_expr(expr))?;
                        }

                        match usage {
                            Once => gen_map_loop(&data_syms, elem_ty, body, &builder_sym,
                                                 b_sym, i_sym, e_sym, glob_ctx, local_ctx),
                            AtMostOnce =>
                                gen_filter_loop(&data_syms, elem_ty, body, &builder_sym,
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
            if let Type::Builder(ref kind, _) = expr.ty {
                use ast::BuilderKind::*;
                match *kind {
                    Merger(ref elem_ty, _) => gen_new_merger(elem_ty, glob_ctx, None),

                    VecMerger(ref elem_ty, _) => {
                        let vec_sym = gen_expr(builder_expr.as_ref().unwrap(),
                                               glob_ctx, local_ctx)?;
                        gen_new_vecmerger(&vec_sym, elem_ty, glob_ctx)
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

            if let Type::Builder(ref kind, _) = builder.ty {
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
                            let (index_sym, merge_val_sym) = {
                                let values = glob_ctx.structs.get(&value_res).unwrap();
                                (values[0].clone(), values[1].clone())
                            };

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
                                index = gen_sym(&index_sym),
                                start = gen_sym(&vecmerger_state.range_start_sym),
                                range_size = gen_sym(&vecmerger_state.range_size_sym),
                                sram = gen_sym(&vecmerger_state.sram_sym),
                                binop = binop,
                                value = gen_sym(&merge_val_sym)
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
                                // In a "map" operation, a "merge" is just an assignment (to the
                                // same offset).
                                add_code!(glob_ctx, "{sram}({ii}) = {value}",
                                          sram = gen_sym(&sram_sym),
                                          ii = gen_sym(&ii_sym),
                                          value = gen_sym(&value_res));
                                Ok(builder_sym)
                            }

                            Filter { ref fifo_sym } => {
                                // In a "filter" operation, a "merge" (hopefully surrounded by a
                                // conditional) entails pushing an element into a local queue.
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
            if let Type::Builder(ref kind, _) = builder.ty {
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
            let res_syms: Vec<Symbol> =
                elems.iter()
                     .map(|child_expr| gen_expr(child_expr, glob_ctx, local_ctx))
                     .collect::<WeldResult<Vec<Symbol>>>()?;

            let struct_sym = glob_ctx.add_variable(); // Just an identifier for the struct.
            glob_ctx.structs.insert(struct_sym.clone(), res_syms);
            Ok(struct_sym)
        }

        ExprKind::GetField { expr: ref struct_expr, index } => {
            let expr_res = gen_expr(&struct_expr, glob_ctx, local_ctx)?;
            if let Some(components) = glob_ctx.structs.get(&expr_res) {
                if let Some(component_sym) = components.get(index as usize) {
                    Ok(component_sym.clone())
                } else {
                    weld_err!("Struct index out of bounds: {}", print_expr(&expr))
                }
            } else {
                weld_err!("Struct value not found: {}", print_expr(&struct_expr))
            }
        }

        _ => {
            weld_err!("Not supported: {}", print_expr(&expr))
        }
    }
}

/// For each array: declares a SRAM and loads block (start::start+extent) into the block.
/// Each SRAM can hold blk elements. Returns the SRAM symbols corresponding to the array symbols.
fn gen_load_arr_blocks(data_syms: &Vec<Symbol>, blk: usize, start: &str, extent: &str,
                       glob_ctx: &mut GlobalCtx) -> WeldResult<Vec<Symbol>> {
    let block_syms = glob_ctx.add_variables(data_syms.len());
    // Declare the local SRAMs.
    for (data_sym, block_sym) in data_syms.iter().zip(block_syms.iter()) {
        let data_ty = &glob_ctx.get_vec_info(&data_sym).elem_ty;
        let data_ty_scala = gen_scalar_type(data_ty, format!(
                "Not supported vector elem type: {}", print_type(data_ty)))?;
        add_code!(glob_ctx, "val {} = SRAM[{}]({})", gen_sym(&block_sym), data_ty_scala, blk);
    }
    // Load into them.
    add_code!(glob_ctx, "Parallel {{");
    for (data_sym, block_sym) in data_syms.iter().zip(block_syms.iter()) {
        add_code!(glob_ctx, "{block} load {arr}({start}::{start}+{extent})",
                  block=gen_sym(&block_sym), arr=gen_sym(&data_sym), start=start, extent=extent);
    }
    add_code!(glob_ctx, "}}  // Parallel");

    Ok(block_syms)
}

/// Declares the `e` parameter for loop body.
fn gen_loop_e(e_sym: &Symbol, block_syms: &Vec<Symbol>, loop_var: &str, glob_ctx: &mut GlobalCtx) {
    assert!(!block_syms.is_empty());
    if block_syms.len() == 1 { // `e` is just an element.
        add_code!(glob_ctx, "val {} = {}({})", gen_sym(&e_sym), gen_sym(&block_syms[0]), loop_var);
    } else { // `e` is a struct of elements.
        let component_syms = glob_ctx.add_variables(block_syms.len());
        for (block_sym, component_sym) in block_syms.iter().zip(component_syms.iter()) {
            add_code!(glob_ctx, "val {} = {}({})", gen_sym(&component_sym), gen_sym(&block_sym),
                      loop_var);
        }
        glob_ctx.structs.insert(e_sym.clone(), component_syms);
    }
}

/// Codegen: "for" loop over `data_syms` with vecmerger with `binop` into vector `dst_sym`.
///
/// A for loop with a vecmerger may involve random access to a Weld vector (`dst_sym`), which is
/// backed by a Spatial DRAM.  However, a DRAM element has to be brought into an SRAM before being
/// accessed.  We therefore break the DRAM into chunks and execute the loop body for each chunk;
/// a merge is carried out iff it's into an element within the current chunk.  Although this
/// approach may require making multiple passes over `data_syms`, the destination DRAM is hopefully
/// small enough to fit into an SRAM so that only one pass is needed.
///
/// See the diagram in the function body for the implementation of a pass.
fn gen_vecmerger_loop(data_syms: &Vec<Symbol>, dst_ty: &Type, body: &TypedExpr,
                      dst_sym: &Symbol, binop: BinOpKind,
                      b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                      glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    assert!(!data_syms.is_empty());

    const BLK_SIZE_DST: usize = 16; // TODO(zhangwen): tunable parameter.
    const BLK_SIZE_SRC: usize = 32; // TODO(zhangwen): tunable parameter.
    const PAR: i32 = 4;
    let dst_veclen_sym = &glob_ctx.get_vec_info(&dst_sym).len;

    // Merge into blocks of vecmerger, one by one.
    let dst_ty_scala = gen_scalar_type(dst_ty, format!(
            "Not supported vecmerger elem type: {}", print_type(dst_ty)))?;
    let dst_sram_sym = glob_ctx.add_variable();
    let range_start_sym = glob_ctx.add_variable();
    let dst_block_size_sym = glob_ctx.add_variable();
    let data_len_sym = &glob_ctx.get_vec_info(&data_syms[0]).len;
    let local_dst_sram_sym = glob_ctx.add_variable();

    // Generate code to load blocks of data into SRAMs.
    glob_ctx.enter_builder_scope();
    let block_syms = gen_load_arr_blocks(data_syms, BLK_SIZE_SRC, "base", "data_block_size",
                                         glob_ctx)?;
    let load_arrs_code = glob_ctx.exit_builder_scope();

    // Generate body code.
    let mut sub_local_ctx = local_ctx.clone();

    // Initialize `e`.
    glob_ctx.enter_builder_scope();
    gen_loop_e(e_sym, &block_syms, "ii", glob_ctx);
    let init_e_code = glob_ctx.exit_builder_scope();

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
                    val data_block_size =
                        min(max({data_len} - base, 0.to[Index]), {data_blk}.to[Index])
                    {load_arrs_code}

                    Pipe(data_block_size by 1) {{ ii =>
                        // TODO(zhangwen): Spatial indices are 32-bit.
                        val {i_sym} = (base + ii).to[Long]
                        {init_e_code}

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
        load_arrs_code = load_arrs_code,
        data_len = gen_sym(&data_len_sym),
        data_blk = BLK_SIZE_SRC,
        local_dst_sram = gen_sym(&local_dst_sram_sym),
        i_sym = gen_sym(i_sym),
        init_e_code = init_e_code,
        body_code = body_code,
        binop = binop);

    Ok(dst_sym.clone())
}

/// Codegen: "for" loop over `data_syms` with merger `merger_sym` with `binop`.
///
/// Such a loop is simply translated to nested Reduces in Spatial.  The result of the Reduce gets
/// folded into the merger's initial value and stored in a new variable (representing the current
/// value of the merger).  A symbol for this variable is returned.
fn gen_merger_loop(data_syms: &Vec<Symbol>, merger_elem_ty: &Type, body: &TypedExpr,
                   merger_sym: &Symbol, binop: BinOpKind,
                   b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                   glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    assert!(!data_syms.is_empty());

    const BLK_SIZE: usize = 16; // TODO(zhangwen): tunable parameter.
    let reduce_res = glob_ctx.add_variable();
    let merger_type_scala = gen_scalar_type(merger_elem_ty, format!(
            "Not supported merger elem type: {}", print_type(merger_elem_ty)))?;

    // Assuming that the vectors to iterate over have the same length.
    let len_sym = glob_ctx.get_vec_info(&data_syms[0]).len;

    // TODO(zhangwen): Spatial indices are 32-bit.
    // FIXME(zhangwen): do I need Sequential around body_code?
    add_code!(glob_ctx, "\
        val {reduce_res} = Reduce(Reg[{ty}])({veclen} by {blk}){{ i =>
            val block_len = min({veclen} - i, {blk}.to[Index])",
        reduce_res = gen_sym(&reduce_res), ty = merger_type_scala,
        veclen = gen_sym(&len_sym), blk = BLK_SIZE);

    let block_syms = gen_load_arr_blocks(data_syms, BLK_SIZE, "i", "block_len", glob_ctx)?;

    // Go through each element in the block(s).
    add_code!(glob_ctx, "\
        Reduce(Reg[{ty}])(block_len by 1){{ ii =>
            val {i_sym} = (i + ii).to[Long]",
        ty = merger_type_scala, i_sym = gen_sym(&i_sym));

    // Prepare to generate code for body.
    let mut sub_local_ctx = local_ctx.clone();
    gen_loop_e(&e_sym, &block_syms, "ii", glob_ctx);
    // Create new merger for `b`; all local merges are gathered into it.
    gen_new_merger(merger_elem_ty, glob_ctx, Some(b_sym))?;
    let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;

    add_code!(glob_ctx, "\
                {body_res}
            }} {{ _{binop}_ }}  // Reduce
        }} {{ _{binop}_ }} {binop} {init_value}  // Reduce",

        body_res = gen_sym(&body_res),
        binop = binop,
        init_value = gen_sym(&merger_sym));

    Ok(reduce_res)
}

/// Codegen: "for" loop over `data_syms` with appender `appender_sym` that performs a "map".
///
/// This case is simple: Create a result vector of the correct size, go through `data_syms`, and
/// make each "merge" write to the corresponding position in the result vector block currently in
/// SRAM.  "merge" into `b_sym` will behave like this inside the loop body after `b_sym` is
/// registered as an "map" appender in the local context.
fn gen_map_loop(data_syms: &Vec<Symbol>, elem_ty: &Type, body: &TypedExpr,
                appender_sym: &Symbol, b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported appender type: {}",
                                                           print_type(elem_ty)))?;
    // The result of a map operation has the same length as the data.
    let veclen_sym = glob_ctx.duplicate_vec(&data_syms[0], &appender_sym, Some(elem_ty.clone()));

    let sram_dst_sym = glob_ctx.add_variable();
    let ii_sym = glob_ctx.add_variable();

    // Generate code to load blocks of data into SRAMs.
    glob_ctx.enter_builder_scope();
    let block_syms = gen_load_arr_blocks(data_syms, BLK_SIZE, "i", "block_size", glob_ctx)?;
    let load_arrs_code = glob_ctx.exit_builder_scope();

    // Generate body code.
    let mut sub_local_ctx = local_ctx.clone();

    // Initialize `e`.
    glob_ctx.enter_builder_scope();
    gen_loop_e(e_sym, &block_syms, gen_sym(&ii_sym).as_str(), glob_ctx);
    let init_e_code = glob_ctx.exit_builder_scope();

    sub_local_ctx.appenders.insert(
        b_sym.clone(), AppenderState::Map { sram_sym: &sram_dst_sym, ii_sym: &ii_sym });

    glob_ctx.enter_builder_scope();
    let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
    assert_eq!(body_res, *b_sym); // Body should return builder derived from `b`.
    let body_code = glob_ctx.exit_builder_scope();

    const BLK_SIZE: usize = 16;
    add_code!(glob_ctx, "\
        Pipe({veclen} by {blk}) {{ i =>
            val {sram_dst} = SRAM[{ty}]({blk})
            val block_size = min({veclen} - i, {blk}.to[Index])
            {load_arrs_code}
            Pipe(block_size by 1) {{ {ii} =>
                val {i_sym} = (i + {ii}).to[Long]
                {init_e_code}

                Sequential {{
                    {body_code}
                }}
            }}
            {dst}(i::i+block_size) store {sram_dst}
        }}",

        veclen = gen_sym(&veclen_sym),
        blk = BLK_SIZE,
        ty = elem_type_scala,
        sram_dst = gen_sym(&sram_dst_sym),
        load_arrs_code = load_arrs_code,
        ii = gen_sym(&ii_sym),
        i_sym = gen_sym(&i_sym),
        init_e_code = init_e_code,
        body_code = body_code,
        dst = gen_sym(&appender_sym));

    Ok(appender_sym.clone())
}

/// Codegen: "for" loop over `data_syms` with appender `appender_sym` that performs a "filter".
///
/// The input vectors are processed in "superblocks" of `PAR` "blocks" of size `BLK_SIZE`.  For
/// each superblock:
///  - For each block (in parallel), evaluate the loop body over each element; a "merge" pushes the
///    result into a FIFO local to the block;
///  - Sequentially compute the starting index of each FIFO in the destination vector (prefix sum);
///  - For each block (in parallel), store its FIFO's contents to the start index in the
///    destination vector.
///
/// The loops are unrolled for parallelism.  We don't use Spatial's "par" feature because it's hard
/// to create and refer to the FIFOs across loops.
fn gen_filter_loop(data_syms: &Vec<Symbol>, appender_elem_ty: &Type, body: &TypedExpr,
                   appender_sym: &Symbol, b_sym: &Symbol, i_sym: &Symbol, e_sym: &Symbol,
                   glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    assert!(!data_syms.is_empty());

    let elem_type_scala = {
        let err_msg = format!("Not supported appender type: {}", print_type(appender_elem_ty));
        gen_scalar_type(appender_elem_ty, err_msg)?
    };

    const BLK_SIZE: usize = 16;
    const PAR: usize = 4;

    // Make new vector.
    let data_len_sym = glob_ctx.get_vec_info(&data_syms[0]).len;
    let result_veclen_sym = glob_ctx.duplicate_vec_empty(&data_syms[0], &appender_sym,
                                                         appender_elem_ty.clone());

    // Manually unroll for parallelism...
    // FIXME(zhangwen): can't use design space exploration on PAR this way...
    add_code!(glob_ctx, "\
        val round_blk = {blk} * {par}
        Sequential({data_len} by round_blk) {{ i =>",
        blk = BLK_SIZE, par = PAR, data_len = gen_sym(&data_len_sym));

    // Make `PAR` local FIFOs.
    let fifo_syms = glob_ctx.add_variables(PAR);
    for fifo_sym in &fifo_syms {
        add_code!(glob_ctx, "val {fifo} = FIFO[{ty}]({blk})",
                  fifo = gen_sym(&fifo_sym), ty = elem_type_scala, blk = BLK_SIZE);
    }

    // Evaluate condition on blocks in parallel.
    add_code!(glob_ctx, "Parallel {{");
    for (par_id, fifo_sym) in fifo_syms.iter().enumerate() {
        // Generate code to load blocks of data into SRAMs.
        glob_ctx.enter_builder_scope();
        let block_syms = gen_load_arr_blocks(data_syms, BLK_SIZE, "base", "block_size", glob_ctx)?;
        let load_arrs_code = glob_ctx.exit_builder_scope();

        // Generate body code.
        // FIXME(zhangwen): shouldn't need to generate body code `PAR` times...
        let mut sub_local_ctx = local_ctx.clone();

        // Initialize `e`.
        glob_ctx.enter_builder_scope();
        gen_loop_e(e_sym, &block_syms, "ii", glob_ctx);
        let init_e_code = glob_ctx.exit_builder_scope();

        sub_local_ctx.appenders.insert(
            b_sym.clone(), AppenderState::Filter { fifo_sym: &fifo_sym });
        glob_ctx.enter_builder_scope();
        let body_res = gen_expr(body, glob_ctx, &mut sub_local_ctx)?;
        assert_eq!(body_res, *b_sym); // Body should return builder derived from `b`.
        let body_code = glob_ctx.exit_builder_scope();

        add_code!(glob_ctx, "\
        {{  // #{par_id}
            val base = (i + {par_id}*{blk}).to[Index]
            val block_size = min(max({data_len} - base, 0.to[Index]), {blk}.to[Index])
            {load_arrs_code}

            Pipe(block_size by 1) {{ ii =>
                val {i_sym} = (base + ii).to[Long]
                {init_e_code}

                Sequential {{
                    {body_code}
                }}  // Sequential
            }}  // Pipe
        }}  // #{par_id}
        ",

        par_id = par_id,
        blk = BLK_SIZE,
        data_len = gen_sym(&data_len_sym),
        load_arrs_code = load_arrs_code,
        i_sym = gen_sym(&i_sym),
        init_e_code = init_e_code,
        body_code = body_code);
    }
    add_code!(glob_ctx, "}}  // Parallel");

    // Compute prefix sums.
    // TODO(zhangwen): smarter prefix sum.
    let mut count_syms = Vec::new();
    for par_id in 0usize..PAR {
        let count_sym = glob_ctx.add_variable();
        let sum_expr = fifo_syms.iter().take(par_id + 1) // Construct the sum expression.
                                .map(|s| gen_sym(&s) + ".numel")
                                .collect::<Vec<_>>().join("+");
        add_code!(glob_ctx, "val {} = {}", gen_sym(&count_sym), sum_expr);
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

/// Codegen: create new vecmerger into vector `vec_sym`.
///
/// Creates a new vector and copies the entire `vec_sym` over, so that merges into this vecmerger
/// are on top of the initial values in `vec_sym`.
fn gen_new_vecmerger(vec_sym: &Symbol, elem_ty: &Type, glob_ctx: &mut GlobalCtx)
                     -> WeldResult<Symbol> {
    let elem_type_scala = gen_scalar_type(elem_ty, format!("Not supported vecmerger type: {}",
                                                           print_type(elem_ty)))?;
    let vecmerger_sym = glob_ctx.add_variable();
    let veclen_sym = glob_ctx.duplicate_vec(&vec_sym, &vecmerger_sym, None);

    // Copy from `vec_sym` to `vecmerger_sym`. Could do this lazily, but probably not worth it.
    const BLK_SIZE: usize = 16; // TODO(zhangwen): tunable parameter.
    add_code!(glob_ctx, "\
        Pipe({veclen} by {blk}) {{ i =>
            val ss = SRAM[{ty}]({blk})
            ss load {vec}(i::i+{blk})
            {vecmerger}(i::i+{blk}) store ss
        }} // Pipe",

        veclen = gen_sym(&veclen_sym),
        blk = BLK_SIZE,
        ty = elem_type_scala,
        vec = gen_sym(&vec_sym),
        vecmerger = gen_sym(&vecmerger_sym));

    Ok(vecmerger_sym)
}

/// Codegen: create new appender.
///
/// Doesn't actually generate any code, because the code depends on the appender's use pattern (map
/// vs. filter), which is computed later.
fn gen_new_appender(glob_ctx: &mut GlobalCtx, local_ctx: &mut LocalCtx) -> WeldResult<Symbol> {
    let appender_sym = glob_ctx.add_variable();
    // Don't declare underlying DRAM yet.
    local_ctx.appenders.insert(appender_sym.clone(), AppenderState::Fresh);
    Ok(appender_sym)
}

/// Codegen: create new merger with specified name if `merger_sym` is Some; uses new sym otherwise.
///
/// Just creates a Scala value that is the identity for the reduction operator.  This value is
/// immutable; a `merge` into a merger is represented by a new symbol containing the updated value.
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

/// Generates Scala representation of literals.
fn gen_lit(lit: LiteralKind) -> String {
    let (val_str, type_str) = match lit {
        LiteralKind::BoolLiteral(b) => (b.to_string(), "Boolean"),
        LiteralKind::I8Literal(i) => (i.to_string(), "Char"),
        LiteralKind::I32Literal(i) => (i.to_string(), "Int"),
        LiteralKind::I64Literal(i) => (i.to_string(), "Long"),
        LiteralKind::F32Literal(f) => (f.to_string(), "Float"),
        LiteralKind::F64Literal(f) => (f.to_string(), "Double"),
    };
    format!("{}.to[{}]", val_str, type_str)
}

/// Generates Spatial name for symbol.
fn gen_sym(sym: &Symbol) -> String {
    format!("{}_{}", sym.name, sym.id)
}

/// Returns true if a value of Weld type `ty` is a "first-class" value in Spatial.
///
/// For example, a literal or a merger is a "first-class" value in Spatial, but a vector is not.
/// This function currently assists code generation for conditionals: for "first-class" values,
/// emit a mux; otherwise, emit an "if" block.
///
/// FIXME(zhangwen): Currently doesn't allow Weld conditionals over different structs.  Can make
/// struct "first-class" if all of its element types are "first-class" and emit one mux for each
/// element (or more, for nested structs).
fn is_spatial_first_class(ty: &Type) -> bool {
    use ast::Type::*;
    match *ty {
        Scalar(_) => true,
        Builder(ref kind, _) => {
            use ast::BuilderKind::*;
            match *kind {
                Appender(_) | DictMerger(_, _, _) | VecMerger(_, _) => false,
                GroupMerger(_, _) => false,
                Merger(_, _) => true,
            }
        }
        _ => false,
    }
}

/// Returns appender usage, and true if expr returns an appender derived from `appender_sym`.
fn compute_appender_usage(expr: &TypedExpr, appender_sym: &Symbol) -> (AppenderUsage, bool) {
    let mut aliases: HashSet<Symbol> = HashSet::new();
    aliases.insert(appender_sym.clone());
    _compute_appender_usage(expr, &mut aliases)
}

/// `aliases` is a set of aliases of the appender in question.
fn _compute_appender_usage(expr: &TypedExpr, aliases: &mut HashSet<Symbol>)
                          -> (AppenderUsage, bool) {
    match expr.kind {
        ExprKind::Literal(_) => (AppenderUsage::Unused, false),

        ExprKind::Ident(ref sym) => (AppenderUsage::Unused, aliases.contains(sym)),

        ExprKind::Negate(ref expr) => {
            let (expr_usage, expr_ret) = _compute_appender_usage(expr, aliases);
            assert!(!expr_ret);  // Cannot negate an appender.
            (expr_usage, false)
        }

        ExprKind::Let{ ref name, ref value, ref body } => {
            let (value_usage, value_ret) = _compute_appender_usage(value, aliases);
            let mut sub_aliases = aliases.clone();
            // Both cases are important because `name` might shadow another identifier.
            if value_ret { sub_aliases.insert(name.clone()) } else { sub_aliases.remove(name) };
            let (body_usage, body_ret) = _compute_appender_usage(body, &mut sub_aliases);
            (value_usage.compose_seq(body_usage), body_ret)
        }

        ExprKind::BinOp { ref left, ref right, .. } => {
            let (left_usage, left_ret) = _compute_appender_usage(left, aliases);
            let (right_usage, right_ret) = _compute_appender_usage(right, aliases);
            assert!(!left_ret); // BinOp operands cannot be appenders.
            assert!(!right_ret);
            (left_usage.compose_seq(right_usage), false)
        }

        ExprKind::If { ref cond, ref on_true, ref on_false } => {
            let (cond_usage, cond_ret) = _compute_appender_usage(cond, aliases);
            assert!(!cond_ret);  // A bool cannot be an appender.
            let (true_usage, true_ret) = _compute_appender_usage(on_true, aliases);
            let (false_usage, false_ret) = _compute_appender_usage(on_false, aliases);
            if true_ret != false_ret {
                // Unsupported if only one branch returns the appender in question.
                (AppenderUsage::Unsupported, false)
            } else {
                (cond_usage.compose_seq(true_usage.compose_or(false_usage)), true_ret)
            }
        }

        ExprKind::NewBuilder(ref opt) => {
            if let Some(ref e) = *opt {
                let (usage, ret) = _compute_appender_usage(e, aliases);
                assert!(!ret);
                (usage, false)
            } else {
                (AppenderUsage::Unused, false)
            }
        }

        ExprKind::For { ref iters, ref builder, ref func } => {
            let mut usage = AppenderUsage::Unused;
            for iter in iters {
                let (iter_usage, iter_ret) = _compute_appender_usage(iter.data.as_ref(), aliases);
                assert!(!iter_ret);  // A vector cannot be an appender.
                usage = usage.compose_seq(iter_usage)
            }

            let (builder_usage, builder_ret) = _compute_appender_usage(builder, aliases);
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
            let (builder_usage, builder_ret) = _compute_appender_usage(builder, aliases);
            let (value_usage, value_ret) = _compute_appender_usage(value, aliases);
            assert!(!value_ret);  // Assuming that one cannot merge an appender into a builder...

            let mut usage = builder_usage.compose_seq(value_usage);
            if builder_ret {  // We're merging into the appender in question.
                usage = usage.compose_seq(AppenderUsage::Once)
            }
            (usage, builder_ret)
        }

        ExprKind::Res { ref builder } => {
            let (builder_usage, builder_ret) = _compute_appender_usage(builder, aliases);
            if builder_ret {
                // This probably shouldn't happen (taking result of appender)...
                (AppenderUsage::Unsupported, false)
            } else {
                (builder_usage, false)
            }
        }

        ExprKind::GetField { expr: ref struct_expr, index } => {
            let (builder_usage, builder_ret) = _compute_appender_usage(&struct_expr, aliases);
            assert!(!builder_ret); // A struct cannot be an appender...

            // FIXME(zhangwen): but the retrieved field might be the appender.
            if let Type::Struct(ref types) = struct_expr.ty {
                if let Type::Builder(BuilderKind::Appender(_), _) = types[index as usize] {
                    // The appender might be returned, but for now we can't know for sure...
                    (AppenderUsage::Unsupported, false)
                } else {
                    (builder_usage, false)
                }
            } else {
                panic!("GetField not operating on a struct")
            }
        }

        _ => (AppenderUsage::Unsupported, false)
    }
}
