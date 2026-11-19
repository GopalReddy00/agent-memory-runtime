# Contributing

Thanks for your interest! This is a small project; focused pull requests are easiest to review.

## Setup

```bash
git clone https://github.com/GopalReddy00/agent-memory-runtime.git
cd agent-memory-runtime
npm ci
npm run typecheck && npm test
```

## Running the MongoDB tests

The Mongo contract and integration tests need a replica set (multi-document transactions):

```bash
docker run -d --name mongo-rs -p 27017:27017 mongo:7 --replSet rs0 --bind_ip_all
docker exec mongo-rs mongosh --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})'
npm run test:mongo
```

Without `MONGO_URI` these tests are skipped.

## Guidelines

- Every behavior change needs a test. Storage-level guarantees belong in
  `tests/adapter-contract.ts` so every adapter is held to the same contract.
- Concurrency guarantees must be tested with concurrent calls, not just sequential ones.
- Keep the core dependency-free (only `node:crypto`).
- Update `CHANGELOG.md` under an "Unreleased" heading.
- Do not commit secrets, tokens or real customer data.

## Writing a new storage adapter

See [docs/ADAPTERS.md](docs/ADAPTERS.md).
