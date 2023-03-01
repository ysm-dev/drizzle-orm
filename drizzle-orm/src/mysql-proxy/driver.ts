import { DefaultLogger, type Logger } from '~/logger';
import { MySql2QueryResultHKT } from '~/mysql2/session';
import { MySqlDatabase } from '~/mysql-core/db';
import { MySqlDialect } from '~/mysql-core/dialect';
import { MySqlRemoteSession, MySqlQueryResult } from '~/mysql-proxy/session';

export interface DrizzleConfig {
	logger?: boolean | Logger;
}

export interface MySqlRemoteResult<T = unknown> extends MySql2QueryResultHKT, MySqlQueryResult<T> {}

export type MySqlRemoteDatabase = MySqlDatabase<MySqlRemoteResult, MySqlRemoteSession>;

export type AsyncRemoteCallback = (sql: string, params: any[]) => Promise<MySqlQueryResult>;

export type RemoteCallback = AsyncRemoteCallback;

export function drizzle(callback: RemoteCallback, config: DrizzleConfig = {}): MySqlRemoteDatabase {
	const dialect = new MySqlDialect();
	let logger;
	if (config.logger === true) {
		logger = new DefaultLogger();
	} else if (config.logger !== false) {
		logger = config.logger;
	}
	const session = new MySqlRemoteSession(callback, dialect, { logger });
	return new MySqlDatabase(dialect, session);
}
