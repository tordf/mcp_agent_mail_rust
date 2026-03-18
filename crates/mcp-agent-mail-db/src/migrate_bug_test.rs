fn strip_timezone_suffix(s: &str) -> &str {
    let s = s.strip_suffix('Z').unwrap_or(s);
    if s.len() >= 6 && s.is_char_boundary(s.len() - 6) {
        let tail = &s[s.len() - 6..];
        if (tail.starts_with('+') || tail.starts_with('-'))
            && tail[1..3].chars().all(|c| c.is_ascii_digit())
            && tail.as_bytes()[3] == b':'
            && tail[4..6].chars().all(|c| c.is_ascii_digit())
        {
            return &s[..s.len() - 6];
        }
    }
    s
}

fn main() {
    let mut s = String::from("some_timestamp");
    s.push('+');
    s.push('\u{20AC}'); // 3 bytes
    s.push('X');
    s.push('Y');
    println!("String: {:?}", s);
    strip_timezone_suffix(&s);
    println!("No panic");
}
