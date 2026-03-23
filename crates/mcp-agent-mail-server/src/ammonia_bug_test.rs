#[test]
fn test_ammonia_on_markdown() {
    let body = "This is a rust generic: `Box<Vec<String>>`";
    let b = ammonia::Builder::new();
    let sanitized = b.clean(body).to_string();
    assert_eq!(sanitized, body);
}
