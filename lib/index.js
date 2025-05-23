//
// index.js
// @trenskow/pged
//
// Created by Kristian Trenskow on 2019/09/03
// See license in LICENSE.
//

import Puqeue from 'puqeue';
import caseit from '@trenskow/caseit';
import pg from 'pg';
import EventEmitter from '@trenskow/async-event-emitter';
import wait from '@trenskow/wait';

import QueryBuilder from './query-builder.js';

const { Pool } = pg;

let id = 0;

let pgOptions;
let pool;

export default class PGed extends EventEmitter {

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

		super();

		pool = pool || new Pool(pgOptions);

		options.casing = options.casing || {};
		options.casing.db = options.casing.db || 'snake';
		options.casing.js = options.casing.hs || 'camel';
		
		options.connection = options.connection || {};
		options.connection.retry = options.connection.retry || {};
		options.connection.retry.count = options.connection.retry.count || 1;
		options.connection.retry.delay = options.connection.retry.delay || '0s';

		options.defaultPrimaryKey = options.defaultPrimaryKey || 'id';

		this._options = options;

		this._connectionCount = 0;
		this._connectionQueue = new Puqeue();

		this._transactions = options.transactions || {};
		this._transactions.mode = this._transactions.mode = options.transactions.mode || 'readCommitted';
		this._transactions.always = this._transactions.always || false;
		this._transactions.count = 0;

		this._id = id++;

		this._history = [];

		this._cache = {};
		this._cacheQueue = {};
		this._cacheHits = 0;

		this._nullIsUndefined = options.nullIsUndefined !== false;

		this._commit = options.commit !== false;

		this._cacheQueue = new Puqeue();

	}

	get id() {
		return this._id;
	}

	get history() {
		return this._history;
	}

	async _query(query, ...args) {
		let info = {
			query,
			parameters: args[0],
			timing: {
				start: new Date()
			}
		};
		this._history.push(info);
		if (!this._client) {
			console.warn(`We had a query without a client for query: ${query} (${JSON.stringify(...args)}).`);
		}
		const result = await this._client.query(query, ...args);
		info.timing.ms = (new Date()).getTime() - info.timing.start.getTime();
		return result;
	}

	_convertResult(result, options) {
		if ((options || {}).format === 'raw') return (result || {}).rows;
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
			
			let connectionCount = 0;

			while (connectionCount < this._options.connection.retry.count) {
				try {
					connectionCount++;
					this._client = await pool.connect();
					break;
				} catch (error) {

					if (connectionCount == this._options.connection.retry.count) {
						throw error;
					}

					await wait(this._options.connection.retry.delay);

				}
			}

			await this.emit('connected');

		}
	}

	async _release() {
		this._connectionCount--;
		if (this._connectionCount == 0) {
			await this._client.release();
			this._client = undefined;
			await this.emit('disconnected');
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
		let error;
		let result;
		try {
			result = await todo(this);
		} catch (err) {
			error = err;
		}
		await this.release();
		if (error) throw error;
		return result;
	}

	async beginTransaction() {
		await this._connectionQueue.add(async () => {
			this._transactions.count++;
			await this._retain();
			if (this._transactions.count == 1) {
				await this._query('begin;');
				if (this._transactions.mode !== 'readCommitted') {
					await this.set.transactionMode[this._transactions.mode]();
				}
				await this.emit('startedTransaction');
			}
		});
	}

	async endTransaction(err, opt = {}) {
		await this._connectionQueue.add(async () => {
			opt.rethrow = opt.rethrow !== false;
			this._transactions.count--;
			if (this._transactions.count == 0) {
				const shouldCommit = typeof err === 'undefined' && this._commit;
				await this._query(shouldCommit ? 'commit;' : 'rollback;');
				await this.emit('endedTransaction', err, shouldCommit);
			}
			await this._release();
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

	get set() {
		return {
			transactionMode: {
				readCommitted: async () => {
					await this._query('set transaction isolation level read committed;');
				},
				repeatableRead: async () => {
					await this._query('set transaction isolation level repeatable read;');
				},
				serializable: async () => {
					await this._query('set transaction isolation level serializable;');
				}
			}
		};
	}

	get connectionCount() {
		return {
			get: async () => {
				return await this._connectionQueue.add(async () => {
					return this._connectionCount;
				});
			}
		};
	}

	async exec(query, parameters, options = {}) {

		let result;

		const todo = async () => result = this._convertResult(await this._query(query, parameters), options);

		await this.emit('preQuery', query, parameters);

		try {
			if (this._transactions.always || options.transaction) await this.transaction(todo);
			else await this.retained(todo);
		} finally {
			await this.emit('query', query, parameters);
		}

		if (options.first === true) {
			return (result || [])[0];
		} else if (options.first) {
			return ((result || [])[0] || {})[options.first];
		}

		return result;

	}

	_queryBuild(table) {
		return new QueryBuilder(table, this._options, this);
	}

	from(table) {
		return this._queryBuild(table);
	}

	into(table) {
		return this._queryBuild(table);
	}

};
