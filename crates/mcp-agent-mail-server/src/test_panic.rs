fn main() {
    let name4 = "R🦀ven";
    let prefix4 = "Ro";
    let _ = &name4.as_bytes()[..prefix4.len()];
    println!("No panic");
}