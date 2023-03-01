import { type MigrationConfig, readMigrationFiles } from '~/migrator';
import type { MySqlRemoteDatabase } from '~/mysql-proxy/driver';

export async function migrate(db: MySqlRemoteDatabase, config: string | MigrationConfig) {
	const migrations = readMigrationFiles(config);
	await db.dialect.migrate(migrations, db.session);
}
