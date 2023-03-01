import {
	Connection,
	FieldPacket,
	OkPacket,
	Pool,
	QueryOptions,
	ResultSetHeader,
	RowDataPacket,
} from 'mysql2/promise';

import { type Logger, NoopLogger } from '~/logger';
import type { MySqlDialect } from '~/mysql-core/dialect';
import type { SelectFieldsOrdered } from '~/mysql-core/query-builders/select.types';
import {
	PreparedQuery as PreparedQueryBase,
	type PreparedQueryConfig as PreparedQueryConfigBase,
	MySqlSession,
} from '~/mysql-core/session';
import type { RemoteCallback } from '~/mysql-proxy/driver';
import { fillPlaceholders, type Query } from '~/sql';
import { mapResultRow } from '~/utils';

export type MySql2Client = Pool | Connection;

export type MySqlRawQueryResult = [ResultSetHeader, FieldPacket[]];
export type MySqlQueryResultType =
	| RowDataPacket[][]
	| RowDataPacket[]
	| OkPacket
	| OkPacket[]
	| ResultSetHeader;
export type MySqlQueryResult<T = any> = [T extends ResultSetHeader ? T : T[], FieldPacket[]];

export class PreparedQuery<T extends PreparedQueryConfig> extends PreparedQueryBase<T> {
	private rawQuery: QueryOptions;
	private query: QueryOptions;

	constructor(
		private client: RemoteCallback,
		queryString: string,
		private params: unknown[],
		private logger: Logger,
		private fields: SelectFieldsOrdered | undefined,
		name: string | undefined,
	) {
		super();
		this.rawQuery = {
			sql: queryString,
			// rowsAsArray: true,
			typeCast: function (field: any, next: any) {
				if (
					field.type === 'TIMESTAMP' ||
					field.type === 'DATETIME' ||
					field.type === 'DATE'
				) {
					return field.string();
				}
				return next();
			},
		};
		this.query = {
			sql: queryString,
			rowsAsArray: true,
			typeCast: function (field: any, next: any) {
				if (
					field.type === 'TIMESTAMP' ||
					field.type === 'DATETIME' ||
					field.type === 'DATE'
				) {
					return field.string();
				}
				return next();
			},
		};
	}

	async execute(
		placeholderValues: Record<string, unknown> | undefined = {},
	): Promise<T['execute']> {
		const params = fillPlaceholders(this.params, placeholderValues);

		this.logger.logQuery(this.rawQuery.sql, params);

		const { fields } = this;
		if (!fields) {
			return this.client(this.rawQuery.sql, params);
		}

		const result = this.client(this.query.sql, params);

		return result.then((result) =>
			result.map((row) => mapResultRow<T['execute']>(fields, row, this.joinsNotNullableMap)),
		);
	}

	async all(placeholderValues: Record<string, unknown> | undefined = {}): Promise<T['all']> {
		const params = fillPlaceholders(this.params, placeholderValues);
		this.logger.logQuery(this.rawQuery.sql, params);
		return this.client(this.rawQuery.sql, params);
	}
}

export interface MySqlRemoteSessionOptions {
	logger?: Logger;
}

type PreparedQueryConfig = Omit<PreparedQueryConfigBase, 'statement' | 'run'>;

export class MySqlRemoteSession extends MySqlSession {
	private logger: Logger;

	constructor(
		private client: RemoteCallback,
		dialect: MySqlDialect,
		options: MySqlRemoteSessionOptions = {},
	) {
		super(dialect);
		this.logger = options.logger ?? new NoopLogger();
	}

	prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
		query: Query,
		fields: SelectFieldsOrdered | undefined,
		name: string | undefined,
	): PreparedQueryBase<T> {
		return new PreparedQuery(this.client, query.sql, query.params, this.logger, fields, name);
	}

	async query(query: string, params: unknown[]): Promise<MySqlQueryResult> {
		this.logger.logQuery(query, params);
		const result = await this.client(query, params);
		return result;
	}

	async queryObject(query: string, params: unknown[]): Promise<MySqlQueryResult> {
		return this.client(query, params);
	}
}
