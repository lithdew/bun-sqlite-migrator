import { Database } from "bun:sqlite";
import fs from "node:fs";
import path from "node:path";

export const DEFAULT_MIGRATION_TABLE = "migrations";
export const DEFAULT_ALLOW_UNORDERED_MIGRATIONS = false;

/**
 * Type for the {@link NO_MIGRATIONS} constant. Never create one of these.
 */
export interface NoMigrations {
  readonly __noMigrations__: true;
}

export const NO_MIGRATIONS: NoMigrations = Object.freeze({
  __noMigrations__: true,
});

type DrainOuterGeneric<T> = [T] extends [unknown] ? T : never;
type ShallowRecord<K extends keyof any, T> = DrainOuterGeneric<{
  [P in K]: T;
}>;

interface Migration {
  up(db: Database): void;
  /**
   * An optional down method.
   *
   * If you don't provide a down method, the migration is skipped when
   * migrating down.
   */
  down?(db: Database): void;
}

interface MigrationInfo {
  /**
   * Name of the migration.
   */
  name: string;

  /**
   * The actual migration.
   */
  migration: Migration;

  /**
   * When was the migration executed.
   *
   * If this is undefined, the migration hasn't been executed yet.
   */
  executedAt?: Date;
}

interface NamedMigration extends Migration {
  readonly name: string;
}

interface MigrationState {
  // All migrations sorted by name.
  readonly migrations: ReadonlyArray<NamedMigration>;

  // Names of executed migrations sorted by execution timestamp
  readonly executedMigrations: ReadonlyArray<string>;

  // Name of the last executed migration.
  readonly lastMigration?: string;

  // Migrations that have not yet ran
  readonly pendingMigrations: ReadonlyArray<NamedMigration>;
}

type MigrationDirection = "Up" | "Down";

interface MigrationResultSet {
  /**
   * This is defined if something went wrong.
   *
   * An error may have occurred in one of the migrations in which case the
   * {@link results} list contains an item with `status === 'Error'` to
   * indicate which migration failed.
   *
   * An error may also have occurred before Kysely was able to figure out
   * which migrations should be executed, in which case the {@link results}
   * list is undefined.
   */
  readonly error?: unknown;

  /**
   * {@link MigrationResult} for each individual migration that was supposed
   * to be executed by the operation.
   *
   * If all went well, each result's `status` is `Success`. If some migration
   * failed, the failed migration's result's `status` is `Error` and all
   * results after that one have `status` ´NotExecuted`.
   *
   * This property can be undefined if an error occurred before Kysely was
   * able to figure out which migrations should be executed.
   *
   * If this list is empty, there were no migrations to execute.
   */
  readonly results?: MigrationResult[];
}

interface MigrationResult {
  readonly migrationName: string;

  /**
   * The direction in which this migration was executed.
   */
  readonly direction: MigrationDirection;

  /**
   * The execution status.
   *
   *  - `Success` means the migration was successfully executed. Note that
   *    if any of the later migrations in the {@link MigrationResult.results}
   *    list failed (have status `Error`) AND the dialect supports transactional
   *    DDL, even the successfull migrations were rolled back.
   *
   *  - `Error` means the migration failed. In this case the
   *    {@link MigrationResult.error} contains the error.
   *
   *  - `NotExecuted` means that the migration was supposed to be executed
   *    but wasn't because an earlier migration failed.
   */
  readonly status: "Success" | "Error" | "NotExecuted";
}

class MigrationResultSetError extends Error {
  readonly #resultSet: MigrationResultSet;

  constructor(result: MigrationResultSet) {
    super();
    this.#resultSet = result;
  }

  get resultSet(): MigrationResultSet {
    return this.#resultSet;
  }
}

function isObject(obj: unknown): obj is ShallowRecord<string, unknown> {
  return typeof obj === "object" && obj !== null;
}

function isFunction(obj: unknown): obj is Function {
  return typeof obj === "function";
}

function isMigration(obj: unknown): obj is Migration {
  return isObject(obj) && isFunction(obj.up);
}

export class FileMigrationProvider {
  constructor(
    private props: {
      migrationFolder: string;
    }
  ) {}

  getMigrations(): Record<string, Migration> {
    const migrations: Record<string, Migration> = {};
    const files = fs.readdirSync(this.props.migrationFolder);

    for (const fileName of files) {
      if (
        fileName.endsWith(".js") ||
        (fileName.endsWith(".ts") && !fileName.endsWith(".d.ts")) ||
        fileName.endsWith(".mjs") ||
        (fileName.endsWith(".mts") && !fileName.endsWith(".d.mts"))
      ) {
        const migration = require(
          path.join(this.props.migrationFolder, fileName)
        );
        const migrationKey = fileName.substring(0, fileName.lastIndexOf("."));

        // Handle esModuleInterop export's `default` prop...
        if (isMigration(migration?.default)) {
          migrations[migrationKey] = migration.default;
        } else if (isMigration(migration)) {
          migrations[migrationKey] = migration;
        }
      }
    }

    return migrations;
  }
}

export class Migrator {
  constructor(
    private readonly props: {
      readonly db: Database;
      readonly provider: FileMigrationProvider;
      readonly migrationTableName?: string;
      readonly allowUnorderedMigrations?: boolean;
    }
  ) {}

  doesTableExists(tableName: string) {
    const stmt = this.props.db.query<{ exists: 0 | 1 }, {}>(
      `select exists(select * from sqlite_master where type = 'table' and name = ?) as "exists"`
    );

    return Boolean(stmt.get(tableName)?.exists);
  }

  private get migrationTable(): string {
    return this.props.migrationTableName ?? DEFAULT_MIGRATION_TABLE;
  }

  private get allowUnorderedMigrations(): boolean {
    return (
      this.props.allowUnorderedMigrations ?? DEFAULT_ALLOW_UNORDERED_MIGRATIONS
    );
  }

  public getMigrations(): ReadonlyArray<MigrationInfo> {
    const executedMigrations = this.doesTableExists(this.migrationTable)
      ? this.props.db
          .query<
            { name: string; timestamp: string },
            {}
          >(`select name, timestamp from ${this.migrationTable}`)
          .all({})
      : [];

    const migrations = this.resolveMigrations();

    return migrations.map(({ name, ...migration }) => {
      const executed = executedMigrations.find((it) => it.name === name);

      return {
        name,
        migration,
        executedAt: executed ? new Date(executed.timestamp) : undefined,
      };
    });
  }

  private getExecutedMigrations(db: Database): ReadonlyArray<string> {
    return db
      .query<{ name: string }, {}>(
        `select name from ${this.migrationTable} order by timestamp, name`
      )
      .all({})
      .map((row) => row.name);
  }

  private getPendingMigrations(
    migrations: ReadonlyArray<NamedMigration>,
    executedMigrations: ReadonlyArray<string>
  ): ReadonlyArray<NamedMigration> {
    return migrations.filter((migration) => {
      return !executedMigrations.includes(migration.name);
    });
  }

  private ensureNoMissingMigrations(
    migrations: ReadonlyArray<NamedMigration>,
    executedMigrations: ReadonlyArray<string>
  ) {
    // Ensure all executed migrations exist in the `migrations` list.
    for (const executed of executedMigrations) {
      if (!migrations.some((it) => it.name === executed)) {
        throw new Error(
          `corrupted migrations: previously executed migration ${executed} is missing`
        );
      }
    }
  }

  private ensureMigrationsInOrder(
    migrations: ReadonlyArray<NamedMigration>,
    executedMigrations: ReadonlyArray<string>
  ) {
    // Ensure the executed migrations are the first ones in the migration list.
    for (let i = 0; i < executedMigrations.length; ++i) {
      if (migrations[i]!.name !== executedMigrations[i]) {
        throw new Error(
          `corrupted migrations: expected previously executed migration ${executedMigrations[i]} to be at index ${i} but ${migrations[i]!.name} was found in its place. New migrations must always have a name that comes alphabetically after the last executed migration.`
        );
      }
    }
  }

  public migrateToLatest(): MigrationResultSet {
    return this.migrate(() => ({ direction: "Up", step: Infinity }));
  }

  private migrate(
    getMigrationDirectionAndStep: (state: MigrationState) => {
      direction: MigrationDirection;
      step: number;
    }
  ): MigrationResultSet {
    try {
      this.ensureMigrationTablesExists();
      return this.runMigrations(getMigrationDirectionAndStep);
    } catch (error) {
      if (error instanceof MigrationResultSetError) {
        return error.resultSet;
      }
      return { error };
    }
  }

  private runMigrations(
    getMigrationDirectionAndStep: (state: MigrationState) => {
      direction: MigrationDirection;
      step: number;
    }
  ): MigrationResultSet {
    const run = (): MigrationResultSet => {
      const state = this.getState(this.props.db);

      if (state.migrations.length === 0) {
        return { results: [] };
      }

      const { direction, step } = getMigrationDirectionAndStep(state);

      if (step <= 0) {
        return { results: [] };
      }

      if (direction === "Down") {
        return this.migrateDown(this.props.db, state, step);
      } else if (direction === "Up") {
        return this.migrateUp(this.props.db, state, step);
      }

      return { results: [] };
    };

    let result!: MigrationResultSet;
    this.props.db.transaction(() => (result = run())).immediate();
    return result;
  }

  private migrateUp(
    db: Database,
    state: MigrationState,
    step: number
  ): MigrationResultSet {
    const migrationsToRun: ReadonlyArray<NamedMigration> =
      state.pendingMigrations.slice(0, step);

    const results: MigrationResult[] = migrationsToRun.map((migration) => {
      return {
        migrationName: migration.name,
        direction: "Up",
        status: "NotExecuted",
      };
    });

    for (let i = 0; i < results.length; i++) {
      const migration = state.pendingMigrations[i]!;

      try {
        migration.up(db);

        db.exec(
          `insert into ${this.migrationTable} (name, timestamp) values (?, ?);`,
          [migration.name, new Date().toISOString()]
        );

        results[i] = {
          migrationName: migration.name,
          direction: "Up",
          status: "Success",
        };
      } catch (error) {
        results[i] = {
          migrationName: migration.name,
          direction: "Up",
          status: "Error",
        };

        throw new MigrationResultSetError({
          error,
          results,
        });
      }
    }

    return { results };
  }

  private migrateDown(
    db: Database,
    state: MigrationState,
    step: number
  ): MigrationResultSet {
    const migrationsToRollback: ReadonlyArray<NamedMigration> =
      state.executedMigrations
        .slice()
        .reverse()
        .slice(0, step)
        .map((name) => {
          return state.migrations.find((it) => it.name === name)!;
        });

    const results: MigrationResult[] = migrationsToRollback.map((migration) => {
      return {
        migrationName: migration.name,
        direction: "Down",
        status: "NotExecuted",
      };
    });

    for (let i = 0; i < results.length; ++i) {
      const migration = migrationsToRollback[i]!;

      try {
        if (migration.down) {
          migration.down(db);

          db.exec(`delete from ${this.migrationTable} where name = ?;`, [
            migration.name,
          ]);

          results[i] = {
            migrationName: migration.name,
            direction: "Down",
            status: "Success",
          };
        }
      } catch (error) {
        results[i] = {
          migrationName: migration.name,
          direction: "Down",
          status: "Error",
        };

        throw new MigrationResultSetError({
          error,
          results,
        });
      }
    }

    return { results };
  }

  private getState(db: Database): MigrationState {
    const migrations = this.resolveMigrations();
    const executedMigrations = this.getExecutedMigrations(db);
    this.ensureNoMissingMigrations(migrations, executedMigrations);
    if (!this.allowUnorderedMigrations) {
      this.ensureMigrationsInOrder(migrations, executedMigrations);
    }

    const pendingMigrations = this.getPendingMigrations(
      migrations,
      executedMigrations
    );

    return Object.freeze({
      migrations,
      executedMigrations,
      lastMigration: executedMigrations.at(-1),
      pendingMigrations,
    });
  }

  private ensureMigrationTablesExists() {
    this.ensureMigrationTableExists();
  }

  private ensureMigrationTableExists(): void {
    this.props.db.exec(
      `create table if not exists ${this.migrationTable} (name varchar(255) not null primary key, timestamp varchar(255) not null) without rowid;`
    );
  }

  private resolveMigrations(): ReadonlyArray<NamedMigration> {
    const allMigrations = this.props.provider.getMigrations();

    return Object.keys(allMigrations)
      .sort()
      .map((name) => ({
        ...allMigrations[name]!,
        name,
      }));
  }
}
