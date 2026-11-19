import { InMemoryStorageAdapter } from "../src/index";
import { runAdapterContractTests } from "./adapter-contract";

runAdapterContractTests("InMemoryStorageAdapter", async () => ({
  adapter: new InMemoryStorageAdapter(),
  cleanup: async () => {},
}));
