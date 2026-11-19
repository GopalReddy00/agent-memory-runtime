# Security Policy

## Supported versions

This project is a developer preview. Only the latest `0.x` release receives fixes.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Report privately using GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository (Security tab -> "Report a vulnerability"), or email
**GurakaGopalReddy@gmail.com**.

Include a description, reproduction steps, and the affected version. You can expect an
acknowledgement within a few days. Because this is a small project run by an individual,
there is no formal SLA.

## Scope and trust model

The runtime enforces its guarantees only when these assumptions hold:

- `PrincipalContext`, `tenantId`, `customerId` and the result of authentication are
  supplied by **your trusted code** (IAM / session layer), never by an LLM or end user.
- `setSessionAuthenticated` is called only from your verified authentication boundary.
- Anything with direct write access to the storage backend can bypass every check.
- The prompt-injection regexes are a supplementary heuristic and are easy to bypass.
  The real defenses are attribute allowlists, per-attribute validators and authority tiers.

Reports that depend on violating these assumptions are out of scope, but documentation
improvements are welcome.
