fn main() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    println!("db_path: {}", db_path.display());

    let init_conn = mcp_agent_mail_db::DbConn::open_file(db_path.display().to_string()).unwrap();
    init_conn
        .execute_raw(mcp_agent_mail_db::schema::PRAGMA_DB_INIT_SQL)
        .unwrap();
    let schema_sql = mcp_agent_mail_db::schema::init_schema_sql_base();
    init_conn.execute_raw(&schema_sql).unwrap();
    init_conn
        .execute_raw("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(init_conn);

    let db_url = format!("sqlite:///{}", db_path.display());
    println!("db_url: {}", db_url);

    let cfg = mcp_agent_mail_db::DbPoolConfig {
        database_url: db_url.clone(),
        min_connections: 1,
        max_connections: 2,
        run_migrations: false,
        warmup_connections: 0,
        ..Default::default()
    };

    let pool = mcp_agent_mail_db::DbPool::new(&cfg).unwrap();
    println!("pool sqlite_path: {}", pool.sqlite_path());

    mcp_agent_mail_cli::context::run_async(async move {
        let cx = asupersync::Cx::current().expect("runtime should provide task context");
        let conn = pool.acquire(&cx).await.unwrap();
        match conn.query_sync("SELECT count(*) FROM projects", &[]) {
            Ok(rows) => {
                for row in rows {
                    println!("Count via pool: {:?}", row.get_as::<i64>(0));
                }
            }
            Err(e) => println!("Query error via pool: {:?}", e),
        }
        Ok::<(), mcp_agent_mail_cli::CliError>(())
    })
    .unwrap();
}
