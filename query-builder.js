'use strict';

const
	caseit = require('@trenskow/caseit');

module.exports = exports = class QueryBuilder {

	constructor(table, options = {}, executor) {

		this._options = options;

		options.casing = options.casing || {};
		options.casing.db = options.casing.db || 'snake';
		options.casing.js = options.casing.hs || 'camel';

		this._table = this._dbCase(table);

		this._options.casing = {};
		this._options.casing.db = 'snake';
		this._options.casing.js = 'camel';

		this._table = caseit(table, this._options.casing.db);
		this._command = 'SELECT';
		this._selectKeys = ['*'];
		this._sortingKeys = [];

		this._joins = [];
		this._conditions = [];

		this._executor = executor;

		return this;

	}

	then(...args) {
		this._promise.then.apply(this._promise, args);
	}

	catch(onRejected) {
		super.catch(onRejected);
	}

	_dbCase(input) {
		return input
			.split('.')
			.map((part) => caseit(part, this._options.casing.db))
			.join('.');
	}

	select(keys = ['*']) {
		if (!Array.isArray(keys)) keys = keys.split(/, ?/);
		this._command = 'SELECT';
		this._selectKeys = keys;
		return this;
	}

	async count(key = 'id') {
		this._command = 'SELECT';
		this._selectKeys = [`:COUNT(${this._table}.${this._dbCase(key)}) AS count`];
		return await this.first('count');
	}

	_deductKeyValues(keysAndValues) {
		if (!keysAndValues) throw new TypeError('Keys and values must be provided.');
		if (typeof values !== 'object') throw new TypeError('Keys and values must be an object.');
		let keys = [];
		let values = [];
		Object.keys(keysAndValues).forEach((key) => {
			keys.push(key);
			values.push(keysAndValues[key]);
		});
		return [keys, values];
	}

	async update(keysAndValues) {
		this._command = 'UPDATE';
		[this._updateKeys, this._updateValues] = this._deductKeyValues(keysAndValues);
		return await this._exec();
	}

	async insert(keysAndValues) {
		this._command = 'INSERT';
		[this._insertKeys, this._insertValues] = this._deductKeyValues(keysAndValues);
		return await this._exec();
	}

	async delete() {
		this._command = 'DELETE';
		return await this._exec();
	}

	sorted(keys) {
		if (!Array.isArray(keys)) keys = keys.split(/, ?/);
		this._sortingKeys = keys;
		return this;
	}

	offsetBy(offset) {
		this._offset = offset;
		return this;
	}

	limitTo(limit) {
		this._limit = limit;
		return this;
	}

	paginated(options = {}) {
		this._offset = options.offset || 0;
		this._limit = options.limit || options.count || 0;
		return this;
	}

	join(options) {
		if (!options) throw new TypeError('Options is required.');
		if (!Array.isArray(options)) options = [options];
		options.forEach((options) => {
			if (!options.table) throw new TypeError('Must suppy foreign table: `options.table`.');
			if (!options.foreign) throw new TypeError('Must supply foreign key: `options.foreign`.');
			if (!options.local) throw new TypeError('Must supply local key: `options.local`.');
			this._joins.push(options);
		});
		return this;
	}

	_deductConditions(conditions) {
		if (!conditions) throw new TypeError('Conditions must be provided.');
		if (Array.isArray(conditions)) {
			return conditions.map((conditions) => {
				return this._deductConditions(conditions);
			});
		} else {
			if (typeof conditions !== 'object') throw new TypeError('Conditions must be an object.');
			return Object.keys(conditions).map((key) => {
				let obj = {};
				const dbKey = this._dbCase(key);
				if (conditions[key] == null) {
					obj[dbKey] = null;
				} else if (typeof conditions[key] === 'object' && !(conditions[key] instanceof Date)) {
					obj[dbKey] = this._deductConditions(conditions[key]);
				} else {
					obj[this._dbCase(key)] = conditions[key];
				}
				return obj;
			});
		}
	}

	where(conditions) {
		this._conditions = this._deductConditions(conditions);
		return this;
	}

	async value() {
		return await this._exec();
	}

	async first(key) {
		this._limit = 1;
		if (key) this._first = key;
		else this._first = true;
		return await this._exec();
	}

	_buildKeys(keys) {
		return keys.map((key) => {
			if (key.substr(0,1) == ':') return key.substr(1);
			let as = key.split(':');
			if (as.length == 1) return this._dbCase(as[0]);
			return `${this._dbCase(as[0])} AS ${this._dbCase(as[1])}`;
		}).join(', ');
	}

	_buildConditions(conditions, operator = 'AND', comparer = '=', wrap = true) {
		if (!conditions) throw new TypeError('No conditions provided.');
		return (wrap ? '(' : '') + conditions.map((condition) => {
			let key = Object.keys(condition)[0];
			if (key.substr(0, 1) == '$') {
				switch (key) {
				case '$or':
					return this._buildConditions(condition[key], 'OR', comparer);
				case '$and':
					return this._buildConditions(condition[key], 'AND', comparer);
				case '$lt':
					return this._buildConditions(condition[key], operator, '<', false);
				case '$lte':
					return this._buildConditions(condition[key], operator, '<=', false);
				case '$gt':
					return this._buildConditions(condition[key], operator, '>', false);
				case '$gte':
					return this._buildConditions(condition[key], operator, '>=', false);
				case '$regexp':
					return this._buildConditions(condition[key], operator, '~*', false);
				case '$ne':
					return this._buildConditions(condition[key], operator, '!=', false);
				case '$isnot':
					return this._buildConditions(condition[key], operator, 'IS NOT', false);
				default:
					throw new TypeError(`Unknown operator ${key}.`);
				}
			}
			if (key.substr(0, 1) == ':') {
				return `${key.substr(1)} ${comparer} ${this._dbCase(condition[key])}`;
			}
			this._queryParameters.push(condition[key]);
			if (key.indexOf('.') == -1) key = `"${key}"`;
			return `${key} ${comparer} $${this._queryParameters.length}`;
		}).filter((part) => part.length).join(` ${operator} `) + (wrap ? ')' : '');
	}

	_buildWhere() {
		if (!this._conditions.length) return;
		return `WHERE ${this._buildConditions(this._conditions)}`;
	}

	_buildJoins() {
		if (!this._joins.length) return;
		return this._joins.map((join) => {
			let local = join.local.substr(0,1) == ':' ? this._dbCase(join.local.substr(1)) : `${this._table}.${this._dbCase(join.local)}`;
			let foreign = join.foreign.substr(0,1) == ':' ? this._dbCase(join.foreign.substr(1)) : `${this._dbCase(join.table)}.${this._dbCase(join.foreign)}`;
			return `INNER JOIN ${this._dbCase(join.table)} ON ${local} = ${foreign}`;
		}).join(' ');
	}

	_buildSorting() {
		if (!this._sortingKeys.length) return;
		return `ORDER BY ${this._sortingKeys.map((key) => {
			if (key.substr(0, 1) == '-') return `${this._dbCase(key.substr(1))} DESC`;
			return this._dbCase(key);
		}).join(', ')}`;
	}

	_buildOffset() {
		if (!this._offset) return;
		return `OFFSET ${this._offset}`;
	}

	_buildLimit() {
		if (!this._limit) return;
		return `LIMIT ${this._limit}`;
	}

	_buildUpdate() {
		return `SET ${this._updateKeys.map((key, idx) => {
			let value = this._updateValues[idx];
			if (value.substr(0, 1) == ':') value = value.substr(1);
			else {
				this._queryParameters.push(value);
				value = `$${this._queryParameters.length}`;
			}
			return `${key} = ${value}`;
		}).join(', ')}`;
	}

	_buildInsertValues() {
		return `SET ${this._insertValues.map((value) => {
			this._queryParameters.push(value);
			return `$${this._queryParameters.length}`;
		}).join(', ')}`;
	}

	_build() {

		this._queryParameters = [];

		let parts = [this._command];

		switch (this._command) {
		case 'SELECT':
			parts = parts.concat([
				this._buildKeys(this._selectKeys),
				'FROM',
				this._table,
				this._buildJoins(),
				this._buildWhere(),
				this._buildSorting(),
				this._buildOffset(),
				this._buildLimit()
			]);
			break;
		case 'UPDATE':
			parts = parts.concat([
				this._table,
				this._buildUpdate(),
				this._buildWhere(),
				'RETURNING *'
			]);
			break;
		case 'INSERT':
			parts = parts.concat([
				'INTO',
				this._table,
				this._buildKeys(this._insertKeys),
				this._buildInsertValues(),
				'RETURNING *'
			]);
			break;
		case 'DELETE':
			parts = parts.concat([
				'FROM',
				this._table,
				this._buildWhere()
			]);
			break;
		}

		return [parts.filter((part) => part && part.length).join(' '), this._queryParameters];

	}

	async _exec() {
		return await this._executor(this);
	}

};
