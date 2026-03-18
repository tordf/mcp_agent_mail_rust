use mcp_agent_mail_core::config::parse_dotenv_contents;

fn main() {
    let contents = "FOO=bar\" # not a comment\"";
    let values = parse_dotenv_contents(contents);
    println!("FOO={}", values.get("FOO").unwrap());
}
