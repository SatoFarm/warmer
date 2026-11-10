# warmer

## What?

Like `docker`, but for servers that sleep. Give it a command and a port mapping: the first request boots the backend, and when it sits idle, warmer kills it and keeps the port.

## Why?

My PC is small, but there are a lot of things I want to run. Running all at once burns it. So each model gets a wrapper that stays cold until someone calls it, and I can still call it like it's long-lived.

## Usage

```
warmer run [-d] [--name <n>] [--idle <secs>] -p <host>:<backend> <command...>
warmer ps
warmer logs [-f] <name>
warmer stop <name>
```

`ps` shows `running` (backend up) or `warm` (backend asleep, port still open). `stop` removes the entry for good. Ctrl-C on a foreground `run` is the same as `stop`.
