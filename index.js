'use strict';

const
	Puqeue = require('puqeue'),
	caseit = require('@trenskow/caseit'),
	{ Pool } = require('pg'),
	from = require('@trenskow/from');

const
	QueryBuilder = require('./query-builder');

let id = 0;

let pgOptions;
let pool;

module.exports = exports = class PGed {

	static get pg() {
		return pgOptions;
	}

	static set pg(options) {
		pgOptions = options;
	}

	static async end() {
		if (pool) await pool.end();
		pool = undefined;
	}

	constructor(options = {}) {

		pool = pool || new Pool(pgOptions);

		options.casing = options.casing || {};
		options.casing.db = options.casing.db || 'snake';
		options.casing.js = options.casing.hs || 'camel';

		options.defaultPrimaryKey = options.defaultPrimaryKey || 'id';

		this._options = options;

		this._connectionCount = 0;
		this._connectionQueue = new Puqeue();

		this._transactions = 0;

		this._id = id++;

		this._history = [];

		this._cache = {};
		this._cacheQueue = {};
		this._cacheHits = 0;

		this._nullIsUndefined = options.nullIsUndefined !== false;

		this._commit = options.commit !== false ;

	}

	async _cacheLock(type, todo) {
		this._cacheQueue[type] = this._cacheQueue[type] || new Puqeue({
			name: `pged_${type}_${this._id}`
		});
		return await this._cacheQueue[type].add(todo);
	}

	cache(type) {

		const checkType = (value) => {
			if (!value || Array.isArray(value) || (typeof value !== 'object' && typeof value !== 'function')) {
				throw new TypeError('Value must be an object.');
			}
		};

		const _set = async (values) => {

			let toSet = values;

			if (Array.isArray(toSet.items) && typeof toSet.total === 'number') {
				toSet = toSet.items;
			}

			let wasArray = true;

			if (!Array.isArray(toSet)) {
				wasArray = false;
				toSet = [toSet];
			}

			toSet = toSet.filter((value) => value);

			toSet.forEach(checkType);

			this._cache[type] = this._cache[type] || [];
			this._cache[type].push(...toSet);

			if (!wasArray) return toSet[0];

			return values;

		};

		const _invalidate = async (identifiers) => {
			if (!this._cache[type]) return;
			this._cache[type] = this._cache[type].filter((cacheItem) => {
				return !from(cacheItem).where(identifiers).first();
			});
		};

		return {
			set: async (values) => {
				return this._cacheLock(type, async () => {
					if (typeof values === 'function') values = await Promise.resolve(values());
					return await _set(values);
				});
			},
			get: async (conditions, resolver) => {
				return await this._cacheLock(type, async() => {
					let result = from(this._cache[type] || [])
						.where(conditions)
						.first();
					if (!result) {
						if (resolver) {
							result = await resolver();
							if (result) {
								this._cache[type] = this._cache[type] || [];
								this._cache[type].push(result);
							}
						}
					} else {
						this._cacheHits++;
					}
					return result;
				});

			},
			invalidate: async (identifiers) => {
				await this._cacheLock(type, async () => {
					await _invalidate(identifiers);
				});
			},
			update: async (identifiers, value) => {
				return await this._cacheLock(type, async () => {
					if (typeof value === 'function') value = await Promise.resolve(value());
					await _invalidate(identifiers);
					return await _set([value]);
				});
			},
			patch: async (identifiers, delta) => {
				return await this._cacheLock(type, async () => {
					if (!this._cache[type]) return;
					if (typeof delta === 'function') delta = await Promise.resolve(delta());
					checkType(delta);
					let obj = from(this._cache[type]).where(identifiers).first();
					Object.keys(delta).forEach((key) => {
						obj[key] = delta[key];
					});
					return obj;
				});
			}
		};
		
	}

	get cacheHits() {
		return this._cacheHits;
	}

	get id() {
		return this._id;
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
				if (this._nullIsUndefined && typeof row[key] === 'object' && !row[key]) return;
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
			if (this._transactions == 1) {
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
