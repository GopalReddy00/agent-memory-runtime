import { MongoClient } from "mongodb";
import { describe } from "vitest";
import { MongoStorageAdapter } from "../src/adapters/mongo";
import { runAdapterContractTests } from "./adapter-contract";

const MONGO_URI = process.env.MONGO_URI;

if (!MONGO_URI) {
  describe.skip("MongoStorageAdapter (set MONGO_URI to a replica set to run)", () => {});
} else {
  runAdapterContractTests("MongoStorageAdapter", async () => {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    const db = client.db(`amr_test_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`);
    const adapter = new MongoStorageAdapter(client, db);
    await adapter.ensureIndexes();
    return {
      adapter,
      cleanup: async () => {
        await db.dropDatabase();
        await client.close();
      },
    };
  });
}
