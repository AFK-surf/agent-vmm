use axum::{
    Extension, Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::post,
};
use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};
use tower::ServiceExt;
use vmm_mcp::{Binding, FileSigningKeys, SigningKeys, TokenAuthority};

#[tokio::test]
async fn authentication_tracks_each_token_across_restart_and_rotation() {
    let state = tempfile::tempdir().unwrap();
    let open = || TokenAuthority::new(Arc::new(FileSigningKeys::open(state.path()).unwrap()));
    let issuer = open();
    let one = issuer.issue().unwrap();
    let two = issuer.issue().unwrap();
    assert_ne!(issuer.verify(&one).unwrap(), issuer.verify(&two).unwrap());
    let app = open().protect(Router::new().route(
        "/mcp",
        post(|Extension(binding): Extension<Binding>| async move { binding.allocation_id }),
    ));
    let call = |tokens: Vec<String>| {
        let mut req = Request::post("/mcp");
        for token in tokens {
            req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        app.clone().oneshot(req.body(Body::empty()).unwrap())
    };
    for rejected in [
        vec![],
        vec![one.to_uppercase()],
        vec![one.clone(), two.clone()],
    ] {
        assert_eq!(
            call(rejected).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        call(vec![one.clone()]).await.unwrap().status(),
        StatusCode::OK
    );
    open().rotate().unwrap();
    assert_eq!(
        call(vec![one]).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(vec![two]).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(vec![issuer.issue().unwrap()]).await.unwrap().status(),
        StatusCode::OK
    );
}

#[test]
fn accepts_legacy_key_and_rejects_damaged_state_without_replacing_it() {
    let state = tempfile::tempdir().unwrap();
    let keys = FileSigningKeys::open(state.path()).unwrap();
    keys.replace(&[7; 32]).unwrap();
    let files = state.path().join("appliance-secrets");
    let key = fs::read_dir(files).unwrap().next().unwrap().unwrap().path();
    assert_eq!(fs::read(&key).unwrap(), [7; 32]);
    let auth = TokenAuthority::new(Arc::new(keys));
    let token = "000102030405060708090a0b0c0d0e0f1b3b127eafc0f864b62154c338a7d526";
    assert!(auth.verify(token).unwrap().is_some());
    let mut forged = token.to_owned();
    forged.replace_range(0..2, "ff");
    assert!(auth.verify(&forged).unwrap().is_none());
    fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(FileSigningKeys::open(state.path()).is_err());
    assert_eq!(fs::read(&key).unwrap(), [7; 32]);
}
