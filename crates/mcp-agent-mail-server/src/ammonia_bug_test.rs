use mcp_agent_mail_server::markdown::render_markdown_to_safe_html;

fn main() {
    let md = "<a href=\"javascript:alert(1)\">bad</a>";
    let html = render_markdown_to_safe_html(md);
    println!("HTML: {}", html);
}
