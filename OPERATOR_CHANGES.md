# Operator changes vs original

This document summarizes every meaningful upgrade made to the original
`redis-cluster-operator` in this workstream: correctness/resilience fixes and
PoC security hardening. It is the changelog for reviewers and operators.

TLS, automatic scale/reshard, and storage migration remain **out of scope**.

---

## 1. Original baseline (what we started from)

The original operator already:

- Created a headless Service + StatefulSet for uniform Redis Cluster pods
- Used topology spread (`maxSkew: 1`, `DoNotSchedule`, `NodeTaintsPolicy=Honor`)
- Set `--cluster-announce-ip $(POD_IP)` to avoid stale `?:6379` gossip
- Bootstrapped with placement-aware `planRoles` (no same-node master/replica)
- Assessed health across pods and rebalanced masters after failover
- Reported `Phase` / Ready·Progressing·Degraded conditions
- Rejected unsupported scale/storage changes as `UnsupportedChange`

Known gaps in that baseline (from the architecture review):

| Gap | Severity |
|-----|----------|
| Partial bootstrap never resumed | Major |
| `Ready` ignored Spec role counts | Major |
| Impossible topology / Pending pods looped forever | Major |
| Ghost `CLUSTER NODES` entries never purged | Major |
| Unresolvable master imbalance only logged | Major |
| No Redis auth / NetworkPolicy / pod hardening | Major (security) |
| No scale/reshard / TLS | Product / deferred |

---

## 2. Correctness & resilience (P0 / P1)

### 2.1 Idempotent bootstrap resume

**Files:** `internal/controller/redis.go`, `internal/controller/rediscluster_controller.go`

- Split bootstrap into `ensureMeet`, `ensureSlots`, `ensureReplicas`
- Added `bootstrapOrRepair` / `needsBootstrapOrRepair`
- Resume after mid-flight failure without re-assigning owned slots
- Never demote a slot-owning master during repair
- Treat Redis “slot already busy” as success when resuming

### 2.2 Ready gated on Spec topology

- `rolesMatchSpec` / `countRoles` require:
  - slot-owning masters == `spec.nodes`
  - replicas == `spec.nodes * spec.replicasPerNode`
- Mismatch → `Phase=Degraded`, reason `RoleMismatch`
- Removed unused `observeRoles`

### 2.3 Terminal Failed for impossible layouts

- `planRoles` failure → `Failed` / `InsufficientNodes` (slow requeue)
- Unschedulable pods → `Failed` / `Unschedulable` via `podsUnschedulable`
- Clears back to normal reconcile once pods schedule

### 2.4 Ghost membership heal

- `planMembershipHeal` + `healClusterMembership`
- On incomplete gossip: `CLUSTER FORGET` unknown IPs, `CLUSTER MEET` missing pods
- `myself` is never forgotten

### 2.5 Unresolvable master imbalance surfaced

- `rebalanceMasters` error → `Degraded` / `UnresolvableMasterImbalance`
- No longer log-only

### 2.6 Status detail helper

- `setPhaseDetail` / `updateRedisClusterStatusDetail` allow specific
  condition reasons/messages (`RoleMismatch`, `InsufficientNodes`, etc.)

---

## 3. PoC security hardening

### 3.1 Password authentication

**API:** `spec.passwordSecretRef` (`SecretKeySelector`) on `RedisClusterSpec`

When set:

- Pods get `REDIS_PASSWORD` from the Secret
- Redis flags: `--requirepass`, `--masterauth`, `--protected-mode yes`
- Operator `go-redis` clients use the same password for all CLUSTER commands

When unset:

- Dev-style open Redis (`--protected-mode no`) remains available
- Controller logs that auth is disabled

Missing/empty Secret → `Failed` / `PasswordSecretUnavailable`

**Samples:** `config/samples/rediscluster_secret.yaml`, updated CR sample

### 3.2 NetworkPolicy

- Owned `NetworkPolicy` named like the RedisCluster
- Ingress 6379/16379 from:
  - same cluster pod labels
  - operator pods labeled `control-plane=controller-manager`
- Egress: peer Redis ports + cluster DNS (TCP/UDP 53)
- RBAC: `networking.k8s.io/networkpolicies`

Requires a CNI that enforces NetworkPolicy (otherwise the object is inert).

### 3.3 Pod hardening

- Pod + container `SecurityContext`:
  - `runAsNonRoot: true`, `runAsUser/FSGroup: 999`
  - `allowPrivilegeEscalation: false`
  - drop all capabilities
  - `RuntimeDefault` seccomp

### 3.4 RBAC additions

- `secrets`: `get;list;watch`
- `networkpolicies`: `get;list;watch;create;update;patch;delete`

Regenerated via `make manifests generate`.

---

## 4. Tests added

In `internal/controller/redis_test.go` (among others):

- Bootstrap formation incompleteness / idempotent skip helpers
- `rolesMatchSpec`
- `podsUnschedulable`
- `planMembershipHeal`
- Status detail reason overrides
- Auth flags + SecurityContext on desired StatefulSet
- NetworkPolicy ports / sync
- Redis client carries password

---

## 5. How to enable auth (quick)

```bash
kubectl apply -k config/samples/
# or create your own Secret and set:
# spec.passwordSecretRef.name / .key
```

Unauthenticated clients should then fail `PING` until `AUTH <password>`.

---

## 6. Still not done (explicit)

| Item | Status |
|------|--------|
| TLS / mTLS for Redis | Not implemented |
| Redis ACL multi-user | Not implemented |
| Automatic scale / reshard | Blocked as `UnsupportedChange` |
| PVC / storage-class migration | Blocked as `UnsupportedChange` |
| Validation admission webhooks | Not implemented |
| Production readiness claim | **No** — PoC / isolated clusters only |

---

## 7. Key files touched

| Path | Role |
|------|------|
| `api/v1/rediscluster_types.go` | `passwordSecretRef` |
| `internal/controller/redis.go` | Bootstrap repair, heal, auth-aware clients |
| `internal/controller/rediscluster_controller.go` | Reconcile, STS, NetworkPolicy, status |
| `internal/controller/redis_test.go` | Unit coverage |
| `config/crd/bases/*`, `config/rbac/role.yaml` | Generated CRD/RBAC |
| `config/samples/*` | Secret + CR examples |
| `README.md` | Security/status pointers |
| `OPERATOR_CHANGES.md` | This document |
