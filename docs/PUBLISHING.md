# Publishing checklist (maintainer only; not shipped to npm)

## 0. Decide first
- **Patent?** Publishing (GitHub, npm, social posts) is a public disclosure. In many countries
  that can end your right to patent. Talk to a patent attorney *before* the first push if you care.
- **Employer rights?** Check your employment contract if any of this was written on work time/equipment.
- **License.** MIT is in `LICENSE`. Switch to Apache-2.0 if you want an explicit patent grant.

## 1. Fill in your identity
```bash
npm run set-identity -- --name "GurakaGopalReddy" --email GurakaGopalReddy@gmail.com --github GopalReddy00
# scoped package name (optional):  --package @GopalReddy00/agent-memory-runtime
git diff   # review
```
Also set the date in `CHANGELOG.md` (`## [0.1.0] - YYYY-MM-DD`).

## 2. Verify locally
```bash
npm ci
npm run typecheck && npm test && npm run build && npm run smoke
npm run test:mongo        # needs a Mongo replica set (see CONTRIBUTING.md)
npm pack --dry-run        # should list only dist/, README, LICENSE, CHANGELOG, SECURITY, package.json
```

## 3. GitHub
1. Create an **empty** public repository named `agent-memory-runtime` (no README/license; they exist here).
2. ```bash
   git init -b main && git add . && git commit -m "Initial public release 0.1.0"
   git remote add origin git@github.com:GopalReddy00/agent-memory-runtime.git
   git push -u origin main
   ```
3. Settings -> Security: enable **Private vulnerability reporting** and Dependabot alerts.
4. Settings -> Branches: protect `main` (require the CI checks to pass).
5. Confirm the CI workflow is green (the Mongo job starts a replica set in Docker).

## 4. npm
1. Create an npm account and **enable 2FA**.
2. Check the name is free: `npm view agent-memory-runtime` should return 404 (it was free when this was prepared).
3. Create a **granular access token** (publish permission, limited to this package, or an automation token)
   and add it to the GitHub repo as the secret `NPM_TOKEN`.
4. First publish, either way:
   - **Locally:** `npm login && npm publish` (runs typecheck, tests, build via `prepublishOnly`).
   - **Via GitHub (adds provenance):** create a GitHub Release tagged `v0.1.0`; `publish.yml` verifies the tag
     matches `package.json` and publishes.
   You cannot reuse or overwrite a published version; to fix a mistake, bump the version.

## 5. After publishing
- `npm install your-package` in a scratch directory and run the README quickstart.
- Tag/release `v0.1.0` on GitHub and paste the CHANGELOG entry.
- Do not claim benchmark numbers until you have published a reproducible benchmark.
