use std::sync::Arc;

use datafusion::{
    datasource,
    execution::context::ExecutionContextState,
    physical_plan::{udaf::AggregateUDF, udf::ScalarUDF},
    sql::planner::ContextProvider,
};

use super::information_schema::InfoSchemaTableProvider;

pub struct CubeContext<'a> {
    /// Internal state for the context
    pub state: &'a ExecutionContextState,
}

impl<'a> CubeContext<'a> {
    pub fn new(state: &'a ExecutionContextState) -> Self {
        Self { state }
    }
}

impl<'a> ContextProvider for CubeContext<'a> {
    fn get_table_provider(
        &self,
        name: datafusion::catalog::TableReference,
    ) -> Option<std::sync::Arc<dyn datasource::TableProvider>> {
        let table_path = match name {
            datafusion::catalog::TableReference::Partial { schema, table, .. } => {
                Some(format!("{}.{}", schema, table))
            }
            datafusion::catalog::TableReference::Full {
                catalog,
                schema,
                table,
            } => Some(format!("{}.{}.{}", catalog, schema, table)),
            _ => None,
        };

        if let Some(tp) = table_path {
            if tp.eq_ignore_ascii_case("information_schema.tables") {
                return Some(Arc::new(InfoSchemaTableProvider::new()));
            }
        };

        None
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions.get(name).cloned()
    }
}
