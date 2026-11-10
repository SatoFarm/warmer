use std::collections::{HashMap, VecDeque};
use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, Mutex as AMutex};
use tokio::task::JoinHandle;
use warmer::*;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const LOG_KEEP: usize = 1000;

struct Entry {
    spec: Spec,
    child: AMutex<Option<Child>>, // also the spawn/kill lock
    active: AtomicUsize,
    last: Mutex<Instant>,
    logs: Mutex<VecDeque<String>>,
    tx: broadcast::Sender<String>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

type Db = Arc<Mutex<HashMap<String, Arc<Entry>>>>;

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn save(db: &Db) {
    let mut specs: Vec<Spec> = db.lock().unwrap().values().map(|e| e.spec.clone()).collect();
    specs.sort_by_key(|s| s.created);
    let tmp = home().join("state.json.tmp");
    let write = || -> std::io::Result<()> {
        std::fs::write(&tmp, serde_json::to_vec_pretty(&specs)?)?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?; // env may hold secrets
        std::fs::rename(&tmp, home().join("state.json"))
    };
    if let Err(e) = write() {
        eprintln!("warmerd: save failed: {e}");
    }
}

/// Start time of a pid (field 22 of /proc/pid/stat); distinguishes a live backend from a recycled pid.
fn starttime(pid: i32) -> Option<String> {
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    s.rsplit_once(')')?.1.split_whitespace().nth(19).map(String::from)
}

/// Remember spawned backends so a restarted daemon can reap them if this one dies hard (kill -9, OOM).
fn record_pid(pid: u32) {
    if let Some(t) = starttime(pid as i32) {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(home().join("pids")) {
            let _ = writeln!(f, "{pid} {t}");
        }
    }
}

fn reap_leftovers() {
    let f = home().join("pids");
    for l in std::fs::read_to_string(&f).unwrap_or_default().lines() {
        if let Some((pid, t)) = l.split_once(' ') {
            let Ok(pid) = pid.parse::<i32>() else { continue };
            if starttime(pid).as_deref() == Some(t) {
                unsafe { libc::kill(-pid, libc::SIGKILL) };
            }
        }
    }
    let _ = std::fs::remove_file(f);
}

fn random_name(db: &Db) -> String {
    const A: [&str; 8] = ["brave", "calm", "eager", "fuzzy", "jolly", "lazy", "quick", "sleepy"];
    const N: [&str; 8] = ["otter", "panda", "falcon", "gecko", "koala", "lynx", "newt", "yak"];
    let mut x = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() as usize;
    loop {
        let n = format!("{}_{}", A[x % 8], N[(x / 8) % 8]);
        if !db.lock().unwrap().contains_key(&n) {
            return n;
        }
        x = x.wrapping_mul(1103515245).wrapping_add(12345);
    }
}

fn push_log(e: &Entry, line: String) {
    let mut l = e.logs.lock().unwrap();
    if l.len() == LOG_KEEP {
        l.pop_front();
    }
    l.push_back(line.clone());
    let _ = e.tx.send(line);
}

fn pipe_logs(e: Arc<Entry>, r: impl AsyncRead + Unpin + Send + 'static) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(r).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            push_log(&e, l);
        }
    });
}

async fn kill_child(c: &mut Option<Child>) {
    let Some(mut ch) = c.take() else { return };
    if let Some(pid) = ch.id() {
        unsafe { libc::kill(-(pid as i32), libc::SIGTERM) };
        if tokio::time::timeout(Duration::from_secs(5), ch.wait()).await.is_err() {
            unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
            let _ = ch.wait().await;
        }
    }
}

async fn alive(c: &mut Option<Child>) -> bool {
    matches!(c.as_mut().map(|c| c.try_wait()), Some(Ok(None)))
}

/// Spawn the backend if it is not running, then wait until `backend` port accepts a connection.
async fn connect_backend(e: &Arc<Entry>, backend: u16) -> Result<TcpStream, String> {
    {
        let mut c = e.child.lock().await;
        if !alive(&mut c).await {
            let s = &e.spec;
            let mut cmd = Command::new(&s.cmd[0]);
            cmd.args(&s.cmd[1..])
                .current_dir(&s.cwd)
                .env_clear()
                .envs(s.env.iter().cloned())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .process_group(0);
            let mut ch = cmd.spawn().map_err(|er| format!("spawn {}: {er}", s.cmd[0]))?;
            pipe_logs(e.clone(), ch.stdout.take().unwrap());
            pipe_logs(e.clone(), ch.stderr.take().unwrap());
            record_pid(ch.id().unwrap_or(0));
            *c = Some(ch);
        }
    }
    let start = Instant::now();
    while start.elapsed() < READY_TIMEOUT {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", backend)).await {
            return Ok(s);
        }
        if !alive(&mut *e.child.lock().await).await {
            return Err("backend exited before becoming ready".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err("backend not ready in time".into())
}

async fn serve_conn(e: Arc<Entry>, backend: u16, mut front: TcpStream) {
    e.active.fetch_add(1, SeqCst); // before spawning, so the idle watcher cannot kill under us
    match connect_backend(&e, backend).await {
        Ok(mut back) => {
            let _ = tokio::io::copy_bidirectional(&mut front, &mut back).await;
        }
        Err(er) => push_log(&e, format!("warmerd: {er}")),
    }
    *e.last.lock().unwrap() = Instant::now();
    e.active.fetch_sub(1, SeqCst);
}

async fn idle_watch(e: Arc<Entry>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut c = e.child.lock().await;
        let idle = e.last.lock().unwrap().elapsed() > Duration::from_secs(e.spec.idle_secs);
        if idle && e.active.load(SeqCst) == 0 && c.is_some() {
            kill_child(&mut c).await;
            push_log(&e, "warmerd: idle, backend stopped (warm)".into());
        }
    }
}

/// Bind all ports, then register the entry. Binding first means a failure leaves nothing behind.
async fn add(db: &Db, spec: Spec) -> Result<Arc<Entry>, String> {
    let mut listeners = vec![];
    for p in &spec.ports {
        let l = TcpListener::bind((p.bind.as_str(), p.host)).await.map_err(|er| format!("bind {}:{}: {er}", p.bind, p.host))?;
        listeners.push((l, p.backend));
    }
    let e = Arc::new(Entry {
        spec,
        child: AMutex::new(None),
        active: AtomicUsize::new(0),
        last: Mutex::new(Instant::now()),
        logs: Mutex::new(VecDeque::new()),
        tx: broadcast::channel(256).0,
        tasks: Mutex::new(vec![]),
    });
    let mut tasks = vec![tokio::spawn(idle_watch(e.clone()))];
    for (l, backend) in listeners {
        let e = e.clone();
        tasks.push(tokio::spawn(async move {
            while let Ok((s, _)) = l.accept().await {
                tokio::spawn(serve_conn(e.clone(), backend, s));
            }
        }));
    }
    *e.tasks.lock().unwrap() = tasks;
    db.lock().unwrap().insert(e.spec.name.clone(), e.clone());
    Ok(e)
}

async fn stop(e: &Entry) {
    for t in e.tasks.lock().unwrap().drain(..) {
        t.abort();
    }
    kill_child(&mut *e.child.lock().await).await;
}

async fn send(w: &mut (impl AsyncWriteExt + Unpin), r: &Resp) -> std::io::Result<()> {
    let mut b = serde_json::to_vec(r).unwrap();
    b.push(b'\n');
    w.write_all(&b).await
}

async fn handle(db: Db, s: UnixStream) -> std::io::Result<()> {
    let (r, mut w) = s.into_split();
    let mut line = String::new();
    BufReader::new(r).read_line(&mut line).await?;
    let Ok(req) = serde_json::from_str::<Req>(&line) else { return send(&mut w, &Resp::Err("bad request".into())).await };
    match req {
        Req::Run { name, cmd, ports, idle_secs, cwd, env } => {
            let name = name.unwrap_or_else(|| random_name(&db));
            let res = if cmd.is_empty() || ports.is_empty() {
                Err("need a command and at least one -p".to_string())
            } else if db.lock().unwrap().contains_key(&name) {
                Err(format!("name '{name}' already in use"))
            } else {
                add(&db, Spec { name, cmd, ports, idle_secs, cwd, env, created: now() }).await.map(|e| e.spec.clone())
            };
            match res {
                Ok(spec) => {
                    save(&db);
                    send(&mut w, &Resp::Run(spec)).await
                }
                Err(er) => send(&mut w, &Resp::Err(er)).await,
            }
        }
        Req::Ps => {
            let entries: Vec<_> = db.lock().unwrap().values().cloned().collect();
            let mut rows = vec![];
            for e in entries {
                let running = alive(&mut *e.child.lock().await).await;
                rows.push(Row { spec: e.spec.clone(), running });
            }
            rows.sort_by_key(|r| r.spec.created);
            send(&mut w, &Resp::Ps(rows)).await
        }
        Req::Logs { name, follow } => {
            let Some(e) = db.lock().unwrap().get(&name).cloned() else { return send(&mut w, &Resp::Err(format!("no such entry: {name}"))).await };
            let (backlog, mut rx) = {
                let l = e.logs.lock().unwrap(); // subscribe under the lock so no line is missed or doubled
                (l.clone(), e.tx.subscribe())
            };
            for l in backlog {
                send(&mut w, &Resp::Line(l)).await?;
            }
            while follow {
                match rx.recv().await {
                    Ok(l) => send(&mut w, &Resp::Line(l)).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(_) => break,
                }
            }
            send(&mut w, &Resp::Ok).await
        }
        Req::Stop { name } => {
            let e = db.lock().unwrap().remove(&name);
            match e {
                Some(e) => {
                    stop(&e).await;
                    save(&db);
                    send(&mut w, &Resp::Ok).await
                }
                None => send(&mut w, &Resp::Err(format!("no such entry: {name}"))).await,
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let dir = home();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();

    // single instance: held for the process lifetime
    let lock = std::fs::File::create(dir.join("lock")).unwrap();
    if unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(&lock), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return;
    }
    let sock = dir.join("sock");
    let _ = std::fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock).expect("bind ~/.warmer/sock");

    reap_leftovers();
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let saved: Vec<Spec> = std::fs::read(dir.join("state.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    for s in saved {
        let n = s.name.clone();
        if let Err(er) = add(&db, s).await {
            eprintln!("warmerd: restore {n}: {er}");
        }
    }
    save(&db);

    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    loop {
        tokio::select! {
            Ok((s, _)) = listener.accept() => { tokio::spawn(handle(db.clone(), s)); }
            _ = term.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    // state.json is kept, so the next start restores everything as warm
    let all: Vec<_> = db.lock().unwrap().values().cloned().collect();
    for e in all {
        stop(&e).await;
    }
    let _ = std::fs::remove_file(&sock);
}
