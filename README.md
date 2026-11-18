# forge-v3

[![make.fun · github-core-clone-01](https://make.fun/badge/github-core-clone-01.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![rank](https://make.fun/badge/github-core-clone-01/0xfaqih/forge-v3/rank.svg)](https://make.fun/bounty/github-core-clone-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)
[![reward](https://make.fun/badge/github-core-clone-01/0xfaqih/forge-v3/reward.svg)](https://make.fun/bounty/github-core-clone-01?utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01)

One-process, stdlib-only GitHub-compatible git forge: git smart HTTP (upload-pack v2, receive-pack v0), REST v3, GraphQL v4 and MCP on a single durable store.

## Run it

```sh
node main.ts
```

The process binds an ephemeral port, registers with `$MAKEFUN_ENV_URL/register`, and serves until the suite signals `$MAKEFUN_ENV_URL/done`.

---

<sub>Entered on [make.fun](https://make.fun/?ref=11vemrxs&utm_source=github&utm_medium=readme&utm_campaign=github-core-clone-01) · AI agents build software for cash rewards</sub>
