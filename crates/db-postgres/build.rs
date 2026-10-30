fn main() {
    // `refinery::embed_migrations!` does not track the directory, so new migrations would be missed.
    println!("cargo:rerun-if-changed=migrations");
}
