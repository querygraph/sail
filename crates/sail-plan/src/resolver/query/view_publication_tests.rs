use sail_catalog::command::CatalogCommand;

use super::*;
use crate::catalog::CatalogCommandNode;

fn definition(explicit: bool) -> spec::ViewDefinition {
    spec::ViewDefinition {
        definition: "SELECT a, b FROM input /* retained SQL */".into(),
        input: Box::new(input()),
        columns: explicit.then(|| {
            vec![
                spec::ViewColumnDefinition {
                    name: "雪".into(),
                    comment: Some("first column".into()),
                },
                spec::ViewColumnDefinition {
                    name: "MixedCase".into(),
                    comment: None,
                },
            ]
        }),
        if_not_exists: true,
        replace: false,
        comment: Some("view comment".into()),
        properties: vec![
            ("user".into(), "keep".into()),
            ("view.catalogAndNamespace.numParts".into(), "99".into()),
        ],
    }
}
async fn compare(explicit: bool) -> TestResult {
    let fixture = Fixture::new().await?;
    let definition = definition(explicit);
    let resolver = fixture.resolver();
    let (query, publication) = resolver
        .resolve_view_publication(definition.clone())
        .await?;
    assert_eq!(query.fields, Some(vec!["a".into(), "b".into()]));
    assert_eq!(query.plan.schema().fields().len(), 2);
    let native = resolver
        .resolve_command_plan(
            spec::CommandPlan::new(spec::CommandNode::CreateView {
                view: spec::ObjectName::bare("target"),
                definition,
            }),
            &mut PlanResolverState::new(),
        )
        .await?;
    let LogicalPlan::Extension(extension) = native else {
        return Err("native view did not resolve to catalog command".into());
    };
    let node = extension
        .node
        .as_any()
        .downcast_ref::<CatalogCommandNode>()
        .ok_or("view command type lost")?;
    let CatalogCommand::CreateView { options, .. } = node.command() else {
        return Err("wrong catalog command".into());
    };
    assert_eq!(&publication, options);
    assert_eq!(
        publication.definition,
        "SELECT a, b FROM input /* retained SQL */"
    );
    assert_eq!(publication.comment.as_deref(), Some("view comment"));
    assert!(publication.if_not_exists);
    assert!(!publication.replace);
    assert_eq!(
        publication.columns[0].name,
        if explicit { "雪" } else { "a" }
    );
    assert_eq!(
        publication.columns[0].comment.as_deref(),
        explicit.then_some("first column")
    );
    assert_eq!(
        fixture.calls.load(Ordering::SeqCst),
        0,
        "no physical planning/publication"
    );
    assert_eq!(fixture.resources.drain().await?.registered, 0);
    assert!(
        fixture
            .ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&["target"])
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn view_publication_matches_native_explicit_columns_and_metadata() -> TestResult {
    compare(true).await
}
#[tokio::test]
async fn view_publication_matches_native_inferred_columns_and_metadata() -> TestResult {
    compare(false).await
}
#[tokio::test]
async fn view_publication_rejects_bad_column_count_without_publishing() -> TestResult {
    let fixture = Fixture::new().await?;
    let mut definition = definition(true);
    definition.columns = Some(vec![]);
    let resolver = fixture.resolver();
    let prepared = resolver
        .resolve_view_publication(definition.clone())
        .await
        .err()
        .ok_or("publication accepted count mismatch")?;
    let native = resolver
        .resolve_command_plan(
            spec::CommandPlan::new(spec::CommandNode::CreateView {
                view: spec::ObjectName::bare("target"),
                definition,
            }),
            &mut PlanResolverState::new(),
        )
        .await
        .err()
        .ok_or("native accepted count mismatch")?;
    assert!(matches!(
        prepared,
        crate::error::PlanError::AnalysisError(_)
    ));
    assert_eq!(prepared.to_string(), native.to_string());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.resources.drain().await?.registered, 0);
    Ok(())
}
