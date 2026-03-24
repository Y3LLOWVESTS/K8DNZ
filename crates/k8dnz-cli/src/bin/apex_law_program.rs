#[path = "../cmd/apextrace/apex_law_program/mod.rs"]
mod apex_law_program;

fn main() -> anyhow::Result<()> {
    apex_law_program::main_entry()
}