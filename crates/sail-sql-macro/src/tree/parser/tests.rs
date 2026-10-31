use super::*;

fn rejected(input: DeriveInput, expected: &str) {
    let result = derive_tree_parser(input);
    assert!(result.is_err());
    if let Err(error) = result {
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn custom_variant_rejects_ignored_field_parser_attributes() {
    rejected(
        syn::parse_quote! {
            enum Node {
                #[parser(function = custom)]
                Custom(#[parser(function = field)] Value),
            }
        },
        "unexpected `parser` attribute argument",
    );
}

#[test]
fn variant_rejects_unsupported_attributes() {
    rejected(
        syn::parse_quote! { enum Node { #[parser(label = label)] Custom(Value) } },
        "unexpected `parser` attribute argument",
    );
    rejected(
        syn::parse_quote! { enum Node { #[parser(backtrack)] Custom(Value) } },
        "unexpected `parser` attribute path",
    );
}

#[test]
fn variant_rejects_duplicate_function() {
    rejected(
        syn::parse_quote! {
            enum Node {
                #[parser(function = first, function = second)]
                Custom(Value),
            }
        },
        "duplicated `function` argument for the `parser` attribute",
    );
}

#[test]
fn default_unit_variant_still_requires_a_parser() {
    rejected(
        syn::parse_quote! { enum Node { Unit } },
        "cannot derive `TreeParser` for unit fields",
    );
}
