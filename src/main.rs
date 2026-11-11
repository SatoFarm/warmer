use std::process::{exit, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{unix::OwnedReadHalf, UnixStream};
use warmer::*;

#[derive(Parser)]
#[command(about = "Keep heavy servers cold until needed, like docker run")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Register a backend and open its ports; stays in the foreground unless -d
    Run {
        /// HOST:BACKEND, IP:HOST:BACKEND or PATH:BACKEND (repeatable)
        #[arg(short, long = "publish", required = true)]
        p: Vec<String>,
        /// Run in the background and print the mapping
        #[arg(short, long)]
        detach: bool,
        #[arg(long)]
        name: Option<String>,
        /// Stop the backend (not the ports) after this many idle seconds
        #[arg(long, default_value_t = 300)]
        idle: u64,
        #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// List entries
    Ps,
    /// Show backend output
    Logs {
        #[arg(short, long)]
        follow: bool,
        name: String,
    },
    /// Kill the backend and close its ports; the entry is removed
    Stop { name: String },
}

fn die(m: impl std::fmt::Display) -> ! {
    eprintln!("warmer: {m}");
    exit(1)
}

fn parse_port(s: &str) -> Result<PortMap, String> {
    let bad = || format!("bad -p '{s}', want HOST:BACKEND, IP:HOST:BACKEND or SOCKPATH:BACKEND");
    let (front, b) = s.rsplit_once(':').ok_or_else(bad)?;
    let backend = b.parse().map_err(|_| bad())?;
    if front.contains('/') {
        return Ok(PortMap { bind: String::new(), host: 0, backend, sock: Some(front.into()) });
    }
    let (bind, h) = match front.split(':').collect::<Vec<_>>()[..] {
        [h] => ("127.0.0.1", h),
        [ip, h] => (ip, h),
        _ => return Err(bad()),
    };
    Ok(PortMap { bind: bind.into(), host: h.parse().map_err(|_| bad())?, backend, sock: None })
}

/// Connect to the daemon, spawning it detached if it is not up yet.
async fn connect() -> UnixStream {
    let sock = home().join("sock");
    if let Ok(s) = UnixStream::connect(&sock).await {
        return s;
    }
    let _ = std::fs::create_dir_all(home());
    let log = std::fs::OpenOptions::new().create(true).append(true).open(home().join("warmerd.log")).unwrap_or_else(|e| die(e));
    let exe = std::env::current_exe().ok().and_then(|p| p.parent().map(|d| d.join("warmerd"))).filter(|p| p.exists());
    let mut cmd = std::process::Command::new(exe.unwrap_or("warmerd".into()));
    std::os::unix::process::CommandExt::process_group(&mut cmd, 0);
    cmd.stdin(Stdio::null()).stdout(log.try_clone().unwrap()).stderr(log).spawn().unwrap_or_else(|e| die(format!("cannot start warmerd: {e}")));
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(s) = UnixStream::connect(&sock).await {
            return s;
        }
    }
    die("warmerd did not come up, see ~/.warmer/warmerd.log")
}

async fn call(req: &Req) -> tokio::io::Lines<BufReader<OwnedReadHalf>> {
    let (r, mut w) = connect().await.into_split();
    let mut b = serde_json::to_vec(req).unwrap();
    b.push(b'\n');
    w.write_all(&b).await.unwrap_or_else(|e| die(e));
    BufReader::new(r).lines()
}

async fn next(l: &mut tokio::io::Lines<BufReader<OwnedReadHalf>>) -> Resp {
    match l.next_line().await {
        Ok(Some(s)) => serde_json::from_str(&s).unwrap_or_else(|e| die(e)),
        _ => die("daemon closed the connection"),
    }
}

/// Print log lines until the stream ends.
async fn print_logs(l: &mut tokio::io::Lines<BufReader<OwnedReadHalf>>) {
    loop {
        match next(l).await {
            Resp::Line(s) => println!("{s}"),
            Resp::Err(e) => die(e),
            _ => return,
        }
    }
}

fn ago(t: u64) -> String {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().saturating_sub(t);
    match d {
        0..=59 => format!("{d}s ago"),
        60..=3599 => format!("{}m ago", d / 60),
        3600..=86399 => format!("{}h ago", d / 3600),
        _ => format!("{}d ago", d / 86400),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match Cli::parse().cmd {
        Cmd::Run { p, detach, name, idle, command } => {
            let ports = p.iter().map(|s| parse_port(s)).collect::<Result<Vec<_>, _>>().unwrap_or_else(|e| die(e));
            let cwd = std::env::current_dir().unwrap().to_string_lossy().into_owned();
            let req = Req::Run { name, cmd: command, ports, idle_secs: idle, cwd, env: std::env::vars().collect() };
            let spec = match next(&mut call(&req).await).await {
                Resp::Run(s) => s,
                Resp::Err(e) => die(e),
                _ => die("unexpected reply"),
            };
            println!("{}", spec.name);
            for p in &spec.ports {
                println!("{} -> {}", p.front(), p.backend);
            }
            if detach {
                return;
            }
            let mut logs = call(&Req::Logs { name: spec.name.clone(), follow: true }).await;
            tokio::select! {
                _ = print_logs(&mut logs) => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            // same as `warmer stop`
            let _ = next(&mut call(&Req::Stop { name: spec.name }).await).await;
        }
        Cmd::Ps => {
            let Resp::Ps(rows) = next(&mut call(&Req::Ps).await).await else { die("unexpected reply") };
            let mut t = vec![vec!["NAME".to_string(), "COMMAND".into(), "CREATED".into(), "STATUS".into(), "PORTS".into()]];
            for r in rows {
                let s = r.spec;
                let ports: Vec<_> = s.ports.iter().map(|p| format!("{}->{}", p.front(), p.backend)).collect();
                t.push(vec![s.name, format!("\"{}\"", s.cmd.join(" ")), ago(s.created), (if r.running { "running" } else { "warm" }).into(), ports.join(", ")]);
            }
            let w: Vec<usize> = (0..5).map(|i| t.iter().map(|r| r[i].chars().count()).max().unwrap()).collect();
            for r in t {
                let line: Vec<String> = r.iter().zip(&w).map(|(c, w)| format!("{c:w$}")).collect();
                println!("{}", line.join("  ").trim_end());
            }
        }
        Cmd::Logs { follow, name } => print_logs(&mut call(&Req::Logs { name, follow }).await).await,
        Cmd::Stop { name } => match next(&mut call(&Req::Stop { name: name.clone() }).await).await {
            Resp::Err(e) => die(e),
            _ => println!("{name}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_forms() {
        let t = parse_port("127.0.0.1:8080:8081").unwrap();
        assert_eq!((t.bind.as_str(), t.host, t.backend, t.sock), ("127.0.0.1", 8080, 8081, None));
        let t = parse_port("8080:8081").unwrap();
        assert_eq!((t.bind.as_str(), t.host, t.sock), ("127.0.0.1", 8080, None));
        let s = parse_port("/run/app.sock:8081").unwrap();
        assert_eq!((s.sock.as_deref(), s.backend, s.front().as_str()), (Some("/run/app.sock"), 8081, "/run/app.sock"));
        assert!(parse_port("abc").is_err());
        assert!(parse_port("1:2:3:4").is_err());
        assert!(parse_port("/run/app.sock:x").is_err());
    }
}
