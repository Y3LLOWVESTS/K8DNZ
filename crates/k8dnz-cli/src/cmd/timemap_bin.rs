// crates/k8dnz-cli/src/cmd/timemap.rs
//
// Thin router + module wiring.
// Heavy logic lives in cmd/timemap/*.rs (each kept small for iteration + safety).

mod timemap;

pub use timemap::run;
pub use timemap::TimemapArgs;
