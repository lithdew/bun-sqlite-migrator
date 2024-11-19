# bun-sqlite-migrator

A bun:sqlite port of [kysely](https://github.com/kysely-org/kysely)'s migrator module.

```ts
import { Database } from "bun:sqlite";
import { Migrator, FileMigrationProvider } from "bun-sqlite-migrator";

const sqlite = new Database(filename, {
    create: true,
    readwrite: true,
    safeIntegers: true,
    strict: true,
});

sqlite.exec("PRAGMA foreign_keys = ON");
sqlite.exec("PRAGMA journal_mode = WAL");
sqlite.exec("PRAGMA synchronous = NORMAL");
sqlite.exec("PRAGMA temp_store = MEMORY");
sqlite.exec("PRAGMA cache_size = 10000");
sqlite.exec("PRAGMA mmap_size = 30000000000");

const migrator = new Migrator({
    db: sqlite,
    provider: new FileMigrationProvider({
        migrationFolder: "./migrations"
    }),
});

const { error, results } = migrator.migrateToLatest();

for (const result of results ?? []) {
    if (result.status === "Error") {
        throw new Error(
            `failed to execute migration "${result.migrationName}"`,
            { cause: error }
        );
    }

    console.log(
        `migration "${result.migrationName}" was executed successfully`
    );
}
```