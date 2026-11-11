# warmer

## What?

Like `docker`, but for servers that sleep. Give it a command and a port mapping: the first request boots the backend, and when it sits idle, warmer kills it and keeps the port.

## Why?

My PC is small, but there are a lot of things I want to run. Running all at once burns it. So each model gets a wrapper that stays cold until someone calls it, and I can still call it like it's long-lived.

## Usage

```
warmer run [-d] [--name <n>] [--idle <secs>] -p <host>:<backend> <command...>
warmer run [-d] [--name <n>] [--idle <secs>] -p <sockpath>:<backend> <command...>
warmer ps
warmer logs [-f] <name>
warmer stop <name>
```

`ps` shows `running` (backend up) or `warm` (backend asleep, port still open). `stop` removes the entry for good. Ctrl-C on a foreground `run` is the same as `stop`.

## Examples

```
# foreground, backend on 8081 exposed on 8080, sleeps after 5 min idle
warmer run --idle 300 -p 8080:8081 python3 -m http.server 8081

# detached, named
warmer run -d --name llama -p 9000:9001 llama-server --port 9001 -m model.gguf

# backend on 8081 exposed on a unix socket instead of a port
warmer run --idle 300 -p $HOME/llama.sock:8081 llama-server --port 8081 -m model.gguf

warmer ps            # NAME   STATE    PORT
warmer logs -f llama
warmer stop llama
```
