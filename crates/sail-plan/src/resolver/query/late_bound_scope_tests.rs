use std::sync::Mutex;

use sail_catalog::provider::CreateViewOptions;

use super::*;
use crate::resolver::scope::{VIEW_NAMESPACE_PART, VIEW_NAMESPACE_SIZE, ViewScope};

fn parsed(sql: &str) -> Result<spec::Plan, Box<dyn std::error::Error>> {
    Ok(sail_sql_analyzer::statement::from_ast_statement(
        sail_sql_analyzer::parser::parse_one_statement(sql)?,
    )?)
}

#[tokio::test]
async fn default_scope_keeps_computed_binding_and_cte_precedence() -> TestResult {
    let fixture = Fixture::new().await?;
    for sql in [
        "SELECT a FROM IDENTIFIER(concat('in', 'put'))",
        "WITH input AS (SELECT 1 AS a) SELECT a FROM IDENTIFIER(concat('in', 'put'))",
    ] {
        let named = fixture.resolver().resolve_named_plan(parsed(sql)?).await?;
        assert_eq!(named.fields, Some(vec!["a".into()]));
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

struct LateCatalogOnly<'a> {
    input: &'a LogicalPlan,
    named: AtomicUsize,
    late: Mutex<Vec<Vec<String>>>,
}
impl RelationScope for LateCatalogOnly<'_> {
    fn resolve(&self, _: &[String]) -> PlanResult<Option<LogicalPlan>> {
        self.named.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.input.clone()))
    }
    fn resolve_late_bound(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        self.late
            .lock()
            .map_err(|e| crate::error::PlanError::internal(e.to_string()))?
            .push(name.to_vec());
        Ok(None)
    }

    fn qualify(&self, _: &[String]) -> PlanResult<Vec<String>> {
        Ok(vec![
            "graph_only_catalog".into(),
            "graph_db".into(),
            "input".into(),
        ])
    }

    fn qualify_late_bound(&self, name: &[String]) -> PlanResult<Vec<String>> {
        Ok(name.to_vec())
    }
}

#[tokio::test]
async fn late_target_opt_out_preserves_native_ctes_and_view_namespace() -> TestResult {
    let fixture = Fixture::new().await?;
    let scope = LateCatalogOnly {
        input: &fixture.inputs.0,
        named: AtomicUsize::new(0),
        late: Mutex::new(vec![]),
    };
    let resolver = PlanResolver::new(&fixture.ctx, Arc::new(PlanConfig::default()))
        .with_relation_scope(&scope);
    resolver
        .resolve_named_plan(parsed("SELECT a FROM input")?)
        .await?;
    assert_eq!(scope.named.load(Ordering::SeqCst), 1);
    let error = resolver
        .resolve_named_plan(parsed("SELECT a FROM IDENTIFIER(concat('in', 'put'))")?)
        .await
        .err()
        .ok_or("late target incorrectly used named binding")?;
    assert!(
        error.to_string().contains("TABLE_OR_VIEW_NOT_FOUND"),
        "{error}"
    );
    assert_eq!(scope.named.load(Ordering::SeqCst), 1);
    assert_eq!(scope.late.lock().map_err(|e| e.to_string())?.len(), 1);
    resolver
        .resolve_named_plan(parsed(
            "WITH input AS (SELECT 1 AS a) SELECT a FROM IDENTIFIER(concat('in', 'put'))",
        )?)
        .await?;
    assert_eq!(
        scope.late.lock().map_err(|e| e.to_string())?.len(),
        1,
        "CTE lookup must precede the late hook"
    );
    let manager = fixture.ctx.extension::<CatalogManager>()?;
    manager
        .create_view(
            &["input"],
            CreateViewOptions {
                definition: "SELECT 1 AS catalog_value".into(),
                columns: vec![],
                if_not_exists: false,
                replace: false,
                comment: None,
                properties: vec![],
            },
        )
        .await?;
    let named = resolver
        .resolve_named_plan(parsed("SELECT * FROM IDENTIFIER(concat('in', 'put'))")?)
        .await?;
    assert_eq!(named.fields, Some(vec!["catalog_value".into()]));
    let properties = vec![
        (VIEW_NAMESPACE_SIZE.into(), "2".into()),
        (format!("{VIEW_NAMESPACE_PART}0"), "catalog".into()),
        (format!("{VIEW_NAMESPACE_PART}1"), "db".into()),
    ];
    let view = ViewScope::from_properties(manager.as_ref(), Some(&scope), &properties)?
        .ok_or("view scope absent")?;
    let resolver =
        PlanResolver::new(&fixture.ctx, Arc::new(PlanConfig::default())).with_relation_scope(&view);
    let named = resolver
        .resolve_named_plan(parsed("SELECT * FROM IDENTIFIER(concat('in', 'put'))")?)
        .await?;
    assert_eq!(named.fields, Some(vec!["catalog_value".into()]));
    assert_eq!(
        scope.late.lock().map_err(|e| e.to_string())?.last(),
        Some(&vec!["catalog".into(), "db".into(), "input".into()])
    );
    assert_eq!(scope.named.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    Ok(())
}
