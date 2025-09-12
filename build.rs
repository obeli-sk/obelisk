fn main() {
    let pkg_version = std::env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION must be set");
    println!("cargo:rustc-env=PKG_VERSION={pkg_version}");
}
