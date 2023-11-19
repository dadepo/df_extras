# df_exras

A collection of user defined functions, from your favourite databases, in [DataFusion](https://arrow.apache.org/datafusion/).

## Road to 0.0.1

| **Postgres**    | **[Details](supports/postgres.md)** | 
|-----------------|-------------------------------------|
| Networking      | ✅︎ Done                             |
| Maths           | 🚧︎ Ongoing                         |
| JSON            | ⭘  Not Started                      |
| **Sqlite**      | **[Details](supports/sqlite.md)**   |
| JSON            | 🚧︎ Ongoing                         |
| Built-In Scalar | ⭘  Not Started                      |
| Maths           | ⭘  Not Started                      |

## How to use

Since `df_extras` is not published to crates.io yet, directly specify the git repository link
as a dependency.

```toml
// In Cargo.toml
[dependencies]
datafusion = "33.0.0"
df_extras = { features = ["postgres"], git = "https://github.com/dadepo/df_extras" }
tokio = "1.34.0"
```

Then in code:

```rust
use datafusion::prelude::SessionContext;
use df_extras::postgres::register_postgres_udfs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    register_postgres_udfs(&ctx)?;

    let df = ctx
        .sql("select inet_merge('192.168.1.5/24', '192.168.2.5/24') as result")
        .await?;

    df.show().await?;

    Ok(())
}
```

The above will print out

```
+----------------+
| result         |
+----------------+
| 192.168.0.0/22 |
+----------------+
```