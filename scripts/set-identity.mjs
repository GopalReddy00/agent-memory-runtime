// Fills in your identity everywhere before publishing.
//   npm run set-identity -- --name "Jane Doe" --email jane@example.com --github janedoe [--package @janedoe/agent-memory-runtime]
import { readFileSync, writeFileSync, existsSync } from "node:fs";

const args = Object.fromEntries(
  process.argv.slice(2).reduce((acc, cur, i, all) => (cur.startsWith("--") ? [...acc, [cur.slice(2), all[i + 1]]] : acc), [])
);
for (const k of ["name", "email", "github"]) {
  if (!args[k] || args[k].startsWith("--")) {
    console.error(`Missing --${k}.\nUsage: npm run set-identity -- --name "Jane Doe" --email jane@example.com --github janedoe [--package @scope/name]`);
    process.exit(1);
  }
}

const files = [
  "package.json", "LICENSE", "README.md", "SECURITY.md", "CONTRIBUTING.md", "CHANGELOG.md",
  "docs/ADAPTERS.md", ".github/ISSUE_TEMPLATE/bug_report.yml",
].filter(existsSync);

const OLD_PKG = "agent-memory-runtime";
let total = 0;
for (const f of files) {
  let s = readFileSync(f, "utf8");
  const before = s;
  s = s.split("YOUR NAME").join(args.name)
       .split("you@example.com").join(args.email)
       .split("YOUR_GITHUB_USERNAME").join(args.github);
  if (args.package && args.package !== OLD_PKG) {
    // Rename only npm-facing strings; the GitHub repo name is left alone.
    s = s.split(`from "${OLD_PKG}`).join(`from "${args.package}`)
         .split(`npm install ${OLD_PKG}`).join(`npm install ${args.package}`)
         .split(`npm/v/${OLD_PKG}`).join(`npm/v/${args.package}`)
         .split(`npmjs.com/package/${OLD_PKG}`).join(`npmjs.com/package/${args.package}`)
         .split(`"name": "${OLD_PKG}"`).join(`"name": "${args.package}"`);
  }
  if (s !== before) { writeFileSync(f, s); total++; console.log("updated", f); }
}

const leftovers = files.filter((f) => /YOUR NAME|you@example\.com|YOUR_GITHUB_USERNAME/.test(readFileSync(f, "utf8")));
if (leftovers.length) { console.error("Placeholders remain in:", leftovers.join(", ")); process.exit(1); }
console.log(`Done (${total} files). Review with: git diff`);
