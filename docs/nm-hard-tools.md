# nm-hard-tools

Crucible aggregates the bounded evaluation and benchmark MCP services from
[`neuralmagic/nm-hard-tools`](https://github.com/neuralmagic/nm-hard-tools) behind its privileged
broker. The agent still connects to exactly one MCP endpoint: Crucible's broker. Upstream service
URLs, bearer tokens, Kubernetes authority, and workload submission remain outside the sandbox.

## Deploy the services

`nm-hard-tools` owns its service images and Helm charts. Deploy the required charts separately and
configure their operator-owned target and profile allowlists. Crucible neither installs the charts
nor widens their RBAC or NetworkPolicies.

The broker must be able to reach each configured Kubernetes Service. Use the actual Service name
rendered by Helm.

## Add services to a domain

Declare each upstream under the enabled broker:

```toml
[agent.broker]
enabled = true
bin = "crucible-broker"

[[agent.broker.hard_tool]]
name = "eval"
url = "http://hard-eval-inference-hard-lm-eval-service.eval.svc.cluster.local:8080/mcp"
bearer_token_env = "HARD_EVAL_TOKEN"

[[agent.broker.hard_tool]]
name = "forward"
url = "http://vllm-forward-bench.bench.svc.cluster.local:8001/mcp"
```

When `bearer_token_env` is present, its value must exist in the broker process environment. For a
rendered Kubernetes deployment, source it from a Secret in the deploy profile:

```toml
[[secret_env]]
name = "HARD_EVAL_TOKEN"
secret = "hard-eval-api-token"
key = "token"
```

Do not put service tokens in `[agent.env]`: that table is intentionally exported into the agent
sandbox. A profile `secret_env` reaches the loop process and its broker child without entering the
sandbox.

At startup the broker fails closed if a configured service is unreachable, its token is missing,
or its tool definitions are invalid. It discovers the upstream schemas and exposes each tool as:

```text
mcp__<broker-name>__hard_<service-name>_<upstream-tool-name>
```

For example, lm-eval's `plan_evaluation` becomes
`mcp__broker__hard_eval_plan_evaluation`. Input/output schemas and MCP safety annotations are
preserved. Calls are bounded to a 2 MiB upstream response and redirects are refused.

## Trust boundary

```text
agent sandbox -> Crucible broker -> nm-hard-tools service -> bounded Kubernetes Job
```

Only the first hop is admitted through OpenShell's sandbox egress policy. The broker resolves the
upstream bearer token and translates Crucible's client-facing MCP session into nm-hard-tools'
stateless `2026-07-28` requests. This keeps the existing rule intact: the broker is the sandbox's
only privileged endpoint.
