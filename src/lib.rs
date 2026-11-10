//! Shared protocol: newline-delimited JSON over ~/.warmer/sock.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME not set")).join(".warmer")
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PortMap {
    pub bind: String,
    pub host: u16,
    pub backend: u16,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Spec {
    pub name: String,
    pub cmd: Vec<String>,
    pub ports: Vec<PortMap>,
    pub idle_secs: u64,
    pub cwd: String,
    pub env: Vec<(String, String)>,
    pub created: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Row {
    pub spec: Spec,
    pub running: bool,
}

#[derive(Serialize, Deserialize)]
pub enum Req {
    Run { name: Option<String>, cmd: Vec<String>, ports: Vec<PortMap>, idle_secs: u64, cwd: String, env: Vec<(String, String)> },
    Ps,
    Logs { name: String, follow: bool },
    Stop { name: String },
}

#[derive(Serialize, Deserialize)]
pub enum Resp {
    Run(Spec),
    Ps(Vec<Row>),
    Line(String),
    Ok,
    Err(String),
}
