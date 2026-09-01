# ADR 0006: Key pair authentication for service access

**Status:** Accepted
**Date:** 2026-08
**Referenced by:** `ingestion/src/trade_sim/loaders/snowflake_loader.py`,
[`setup.md`](../setup.md#step-5-generate-and-register-a-key-pair)

---

## Context

Four things connect to Snowflake without a human present: the loader, the reconciler, dbt (from
Airflow), and the dashboard. Each needs credentials that work non-interactively, can be rotated,
and are safe to configure on a laptop as well as in CI.

Three constraints shaped the decision:

1. **Snowflake now enforces MFA for human users.** Any password-based flow for a user with MFA
   enabled requires an interactive challenge, which an Airflow task cannot answer.
2. **Rotation must not require downtime.** A credential that can only be replaced by breaking
   authentication first will not be rotated on schedule, and an unrotated credential is the one that
   leaks.
3. **CI must be able to authenticate** from a GitHub Actions runner without a browser.

## Decision

**Use RSA key pair authentication (JWT) everywhere, for every non-interactive connection.**

The private key lives at `SNOWFLAKE_PRIVATE_KEY_PATH` (default `.secrets/rsa_key.p8`, mode 600,
gitignored). `SnowflakeSession` reads it, and the same code path serves local runs, Airflow, and
CI — where the key is a repository secret written to a temporary file.

`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` is supported but unset locally, so the local key is unencrypted.
`SNOWFLAKE_PASSWORD` remains supported as a documented fallback for a first-run situation where a
key has not yet been registered.

## Alternatives considered

**Username and password.** Simplest, and the only option that needs no setup step. Rejected because
MFA makes it unusable for a service identity, because rotation means a window in which
authentication is broken, and because the secret is transmitted on every connection rather than used
to sign locally.

**OAuth / external browser SSO.** The right answer for *human* access and worth using for the
Snowflake UI. Rejected for service access because it requires an interactive browser redirect, which
is precisely what a scheduled task cannot do.

**Snowflake's key pair with an encrypted key locally.** Considered and rejected for the *local*
setup only: it adds a passphrase to manage during a 30-minute setup with no security benefit on a
single-user laptop, and the passphrase would end up in `.env` next to the key anyway. The code
supports it, and production should use it with the passphrase held in a secrets manager.

**External browser plus a cached token.** Fragile, expires unpredictably, and turns a scheduled
failure into an authentication mystery.

## Consequences

### Good

- Non-interactive by design; no MFA interaction, and the same mechanism works locally, in Airflow
  and in CI.
- **Rotation without downtime.** Snowflake accepts `RSA_PUBLIC_KEY` and `RSA_PUBLIC_KEY_2`
  simultaneously, so the sequence is: register the new key as the second, deploy the new private key,
  unset the first. There is no window in which authentication is broken.
- Nothing reusable crosses the network. The private key signs a JWT locally; the key itself never
  leaves the machine. A captured connection yields a short-lived token, not a credential.
- The fingerprint is queryable (`DESC USER`), so which key a user is configured with is verifiable
  rather than assumed.

### Bad

- **A file on disk is an attack surface**, and an unencrypted one more so. Mitigated locally by mode
  600 in a gitignored directory, plus Gitleaks in CI as a second line of defence — but the honest
  statement is that a laptop with this key can act as the service user.
- One more setup step, and the step most likely to be got wrong: pasting the public key with the PEM
  header or with newlines intact. `make doctor` checks for this specifically, and `make keypair`
  prints the exact string to paste, because the resulting Snowflake error ("incorrect username or
  password") points at entirely the wrong thing.
- Key generation needs `openssl`. Present on macOS and Linux; on Windows it means WSL.

### Neutral

- Terraform creates **three** service users — ingest, dbt, BI — each with its own `rsa_public_key`
  variable and its own functional role, so per-consumer credentials are already the shape of the
  design. Locally all three are given the same public key, because `make keypair` generates one pair
  and managing three on a laptop buys nothing. In production each would get its own from a secrets
  manager, which is a `terraform.tfvars` change and no code change.
- The service users are created with `snowflake_service_user`, not `snowflake_user`. Snowflake's
  service user type is exempt from the MFA enrolment now applied to human users, which is what makes
  this supported rather than merely working for the time being.

## Notes

The rotation procedure, in full:

```sql
-- 1. Add the new key alongside the old one. Both are now valid.
ALTER USER svc_trade_pipeline SET RSA_PUBLIC_KEY_2 = '<new public key>';
-- 2. Deploy the new private key to every consumer. Verify each one connects.
-- 3. Remove the old key.
ALTER USER svc_trade_pipeline UNSET RSA_PUBLIC_KEY;
-- 4. Promote, so the next rotation can reuse the same two slots.
ALTER USER svc_trade_pipeline SET RSA_PUBLIC_KEY = '<new public key>';
ALTER USER svc_trade_pipeline UNSET RSA_PUBLIC_KEY_2;
```

Step 2 is the one that must not be rushed: verify every consumer against the new key before removing
the old, because the old key is the only way back.
