'use strict';

const
	Puqeue = require('puqeue'),
	caseit = require('@trenskow/caseit'),
	{ Pool } = require('pg');

const
	QueryBuilder = require('./query-builder');

let id = 0;

const pool = new Pool({
	user: process.env.PG_USER,
	host: process.env.PG_HOST,
	database: process.env.PG_DATABASE,
	password: process.env.PG_PASSWORD,
	port: process.env.PG_PORT || 5432,
});

module.exports = exports = class Db {

	constructor(options = {}) {

		this._options = options;

		options.casing = options.casing || {};
		options.casing.db = options.casing.db || 'snake';
		options.casing.js = options.casing.hs || 'camel';

		this._connectionCount = 0;
		this._connectionQueue = new Puqeue();

		this._transactions = 0;

		this._id = id++;

		this._history = [];

		this._cache = {};
		this._cacheHits = {};
		this._cacheQueue = {};

		this._commit = options.commit !== false || process.env.PG_COMMIT !== 'false';

	}

	async _cacheLock(type, todo) {
		this._cacheQueue[type] = this._cacheQueue[type] || new Puqeue();
		return await this._cacheQueue[type].add(todo);
	}

	get cache() {
		let result = {
			set: async (type, value) => {
				await this._cacheLock(type, async () => {
					this._cache[type] = this._cache[type] || [];
					this._cache[type].push(value);
				});
			},
			get: async (type, identifiers, resolver) => {
				return await this._cacheLock(type, async() => {
					let result = (this._cache[type] || [])
						.filter((cacheItem) => {
							return Object.keys(cacheItem)
								.some((key) => {
									return Object.keys(identifiers)
										.some((identifierKey) => {
											return cacheItem[key] === identifiers[identifierKey];
										});
								});
						})[0];
					if (!result) {
						if (resolver) {
							result = await resolver();
							if (result) {
								this._cache[type] = this._cache[type] || [];
								this._cache[type].push(result);
							}
						}
					} else{
						this._cacheHits++;
					}
					return result;
				});
			}
		};
		Object.defineProperty(result, 'hits', {
			get: () => {
				return this._cacheHits;
			}
		});
		return result;
	}

	get history() {
		return this._history;
	}

	async _query(query, ...args) {
		this._history.push({
			query,
			parameters: args[0]
		});
		return await this._client.query(query, ...args);
	}

	_convertResult(result) {
		return ((result || {}).rows || []).map((row) => {
			let newRow = {};
			Object.keys(row).forEach((key) => {
				newRow[caseit(key, this._options.casing.js)] = row[key];
			});
			return newRow;
		});
	}

	async _retain() {
		this._connectionCount++;
		if (this._connectionCount == 1) {
			this._client = await pool.connect();
		}
	}

	async _release() {
		this._connectionCount--;
		if (this._connectionCount == 0) {
			this._client.release();
			this._client = undefined;
		}
	}

	async retain() {
		await this._connectionQueue.add(async () => {
			await this._retain();
		});
	}

	async release() {
		await this._connectionQueue.add(async () => {
			await this._release();
		});
	}

	async retained(todo) {
		await this.retain();
		let result = await todo(this);
		await this.release();
		return result;
	}

	async beginTransaction() {
		this._connectionQueue.add(async () => {
			this._transactions++;
			await this._retain();
			if (this._connectionQueue == 1) {
				await this._query('BEGIN;');
			}
		});
	}

	async endTransaction(err, opt = {}) {
		this._connectionQueue.add(async () => {
			opt.rethrow = opt.rethrow !== false;
			this._transactions--;
			if (this._transactions == 0) {
				await this._query(err || !this._commit ? 'ROLLBACK;' : 'COMMIT;');
			}
			this._release();
			if (err && opt.rethrow) throw err;
		});
	}

	async transaction(todo) {
		await this.beginTransaction();
		let error;
		let result;
		try {
			result = await todo(this);
		} catch (err) {
			error = err;
		}
		await this.endTransaction(error);
		return result;
	}

	get connectionCount() {
		return {
			get: async () => {
				return this._connectionQueue.add(async () => {
					return this._connectionCount;
				});
			}
		};
	}

	_exec(table) {
		return new QueryBuilder(table, this._options, async (queryBuilder) => {
			if (queryBuilder._transaction) await this.beginTransaction();
			else await this.retain();
			let [query, parameters] = queryBuilder._build();
			let result = this._convertResult(await this._query(query, parameters));
			if (queryBuilder._transaction) await this.endTransaction();
			else await this.release();
			if (queryBuilder._first === true) {
				return (result || [])[0];
			} else if (queryBuilder._first) {
				return ((result || [])[0] || {})[queryBuilder._first];
			}
			return result;
		});
	}

	from(table) {
		return this._exec(table);
	}

	into(table) {
		return this._exec(table);
	}

};

exports.QueryBuilder = QueryBuilder;
