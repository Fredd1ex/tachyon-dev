#![forbid(unsafe_code)]

//! Compiled-in tool implementations, schemas, and colocated guidance.

pub mod agents;
pub mod artifact;
pub mod browser;
pub mod ctx;
pub mod exec;
pub mod history;
pub mod python;
pub mod services;
#[cfg(test)]
mod services_tests;
pub mod workspace;

pub use browser::agent_browser;
pub use python::ipython;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_schema_budget_stays_compact() {
        let schemas = [ipython(), agent_browser()];
        let chars = schemas
            .iter()
            .map(|schema| {
                schema.name.len() + schema.description.len() + schema.parameters.to_string().len()
            })
            .sum::<usize>();
        assert!(chars < 850, "worker schemas grew to {chars} chars");
    }
}
pub mod work;
