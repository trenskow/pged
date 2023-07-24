import caseit from '@trenskow/caseit';
import CustomPromise from '@trenskow/custom-promise';
import Puqeue from 'puqeue';

const tableInformationQueue = new Puqeue();
const tableInformation = {};

export default class QueryBuilder extends CustomPromise {

	constructor(table, options = {}, connection) {

		super();

		this._options = options;

		options.casing = options.casing || {};
		options.casing.db = options.casing.db || 'snake';
		options.casing.js = options.casing.hs || 'camel';
		options.casing.json = options.casing.json || 'camel';

		options.defaultPrimaryKey = options.defaultPrimaryKey || 'id';

		this._table = this._dbCase(table);

		this._options.casing = {};
		this._options.casing.db = 'snake';
		this._options.casing.js = 'camel';

		this._defaultPrimaryKey = options.defaultPrimaryKey;

		this._table = this._dbCase(table);
		this._sortingKeys = [];

		this._joins = [];
		this._conditions = [];
		this._having = [];

		this._offset = 0;

		this._connection = connection;

	}

	_dbCase(input, quote) {
		return input
			.split(/"|'/)
			.map((part, idx) => {
				if (idx % 2 == 1) return part;
				return part
					.split('.')
					.map((part) => {
						let doQuote = quote;
						if (part.substring(0, 1) === '!' && quote) {
							part = part.substring(1);
							doQuote = false;
						}
						part = part
							.split('->')
							.map((subpart, idx) => caseit(subpart, idx == 0 ? this._options.casing.db : this._options.casing.json))
							.map((subpart) => doQuote ? `"${subpart}"` : subpart)
							.join('->');
						return part;
					})
					.join('.');
			})
			.join('');
	}

	select(keys = ['*']) {
		if (typeof keys !== 'string' || keys[0] !== ':') {
			if (!Array.isArray(keys)) {
				keys = [].concat(...keys.split('"').map((key, idx) => {
					if (idx % 2 == 0) return key.split(/, ?/);
					return [key];
				})).filter((key) => key);
			}
		}
		this._selectKeys = (this._selectKeys || []).concat(keys);
		return this;
	}

	groupBy(element) {
		this._groupBy = element;
		return this;
	}

	count(key = 'id') {
		this._selectKeys = [`:count(${this._table}.${this._dbCase(key)})::int as count`];
		return this.first('count', { select: false });
	}

	_deconstructKeyValues(keysAndValues) {
		if (!keysAndValues) throw new TypeError('Keys and values must be provided.');
		if (typeof keysAndValues !== 'object') throw new TypeError('Keys and values must be an object.');
		let keys = [];
		let values = [];
		Object.keys(keysAndValues).forEach((key) => {
			keys.push(key);
			values.push(keysAndValues[key]);
		});
		return [keys, values];
	}

	update(keysAndValues) {
		this._command = 'update';
		[this._updateKeys, this._updateValues] = this._deconstructKeyValues(keysAndValues);
		this._transaction = true;
		return this.first();
	}

	insert(keysAndValues = {}) {
		this._command = 'insert';
		[this._insertKeys, this._insertValues] = this._deconstructKeyValues(keysAndValues);
		this._transaction = true;
		return this.first();
	}

	delete() {
		this._command = 'delete';
		this._transaction = true;
		return this;
	}

	sorted(sortingKeys) {
		if (typeof sortingKeys === 'string') sortingKeys = sortingKeys.split(/, ?/);
		if (!Array.isArray(sortingKeys)) sortingKeys = [sortingKeys];
		sortingKeys = sortingKeys.map((sortingKey) => {
			if (typeof sortingKey === 'string') return {
				key: sortingKey.substring(0, 1) === '-' ? sortingKey.substring(1) : sortingKey,
				order: sortingKey.substring(0, 1) === '-' ? 'desc' : 'asc'
			};
			return sortingKey;
		});
		this._sortingKeys = this._sortingKeys.concat(sortingKeys);
		return this;
	}

	offsetBy(offset = 0) {
		this._offset = offset;
		return this;
	}

	limitTo(limit) {
		this._limit = limit;
		return this;
	}

	paginated(options) {
		if (!options) return this;
		this.offsetBy(options.offset);
		this.limitTo(options.limit || options.count);
		this._paginated = true;
		return this;
	}

	_areConditions(object) {
		if (Array.isArray(object)) {
			return object.every((value) => typeof value === 'object' && !(value instanceof Date));
		}
		return true;
	}

	_formalizeConditions(conditions) {
		if (!conditions) throw new TypeError('Conditions must be provided.');
		if (Array.isArray(conditions)) {
			return [].concat(...conditions.map((conditions) => {
				return this._formalizeConditions(conditions);
			}));
		} else {
			if (typeof conditions !== 'object') throw new TypeError('Conditions must be an object.');
			return Object.keys(conditions).map((key) => {
				let obj = {};
				const dbKey = this._dbCase(key);
				if (conditions[key] == null) {
					obj[dbKey] = null;
				} else if (typeof conditions[key] === 'object' && this._areConditions(conditions[key]) && !(conditions[key] instanceof Date)) {
					obj[dbKey] = this._formalizeConditions(conditions[key]);
				} else {
					obj[dbKey] = conditions[key];
				}
				return obj;
			});
		}
	}

	where(conditions) {
		this._conditions = this._conditions.concat(this._formalizeConditions(conditions));
		return this;
	}

	join(options) {
		if (!Array.isArray(options)) options = [options];
		this._joins.push(
			...options
				.filter((options) => options)
				.map((options) => {
					if (typeof options !== 'object') throw new TypeError('Option must be an object');
					if (!options.table) throw new SyntaxError('Missing table.');
					if (!options.conditions) {
						options.conditions = {};
						options.local = options.local || this._defaultPrimaryKey;
						options.foreign = options.foreign || this._defaultPrimaryKey;
						let local = options.local.substring(0, 1) == ':' ? this._dbCase(options.local) : `:${this._table}.${this._dbCase(options.local)}`;
						let foreign = options.foreign.substring(0, 1) == ':' ? this._dbCase(options.foreign.substring(1)) : `${this._dbCase(options.table)}.${this._dbCase(options.foreign)}`;
						options.conditions[local] = foreign;
					}
					options.conditions = this._formalizeConditions(options.conditions);
					options.required = options.required || 'both';
					if (!['none', 'local', 'foreign', 'both'].includes(options.required)) {
						throw new TypeError('Only `none`, `local`, `foreign`, `both` are supported by `options.required`.');
					}
					return options;
				}));
		return this;
	}

	first(key, options = { select: true }) {
		this._limit = 1;
		if (key) {
			if (options.select) this.select(key);
			this._first = key;
		}
		else this._first = true;
		return this;
	}

	sum(key) {

		if (key.key) {
			if (key.table) key = `"${this._dbCase(key.table)}"."${this._dbCase(key.key)}"`;
			else key = `"${this._dbCase(key.key)}"`;
		}

		this._selectKeys = [`:sum(${key}) AS sum`];
		this._limit = 1;
		this._first = 'sum';
		this._defaultResult = 0;

		return this;

	}

	onConflict(keys, action) {

		if (this._command !== 'insert') throw new Error('`onConflict` is only available when inserting.');

		if (!Array.isArray(keys)) keys = keys.split(/, ?/);

		switch (Object.keys(action || {})[0] || 'nothing') {
		case 'nothing':
			break;
		case 'update': {
			const [keys, values] = this._deconstructKeyValues(action.update);
			action.update = { keys, values };
			break;
		}
		default:
			throw new Error('Action `update` is only supported at this moment.');
		}

		this._onConflict = {
			keys,
			action
		};

		return this;

	}

	having(conditions) {
		this._having = this._having.concat(this._formalizeConditions(conditions));
		return this;
	}

	_canQuote(key) {
		if (key === '*') return false;
		if (key.toLowerCase().includes(' as ')) return false;
		if (key.includes('(')) return false;
		return true;
	}

	_buildKeys(keys = ['*'], quote) {
		return keys.map((key) => {
			if (key.substring(0, 1) == ':') return key.substring(1);
			let as = key.split(':');
			if (as.length == 1) return this._dbCase(as[0], quote && this._canQuote(key));
			return `${this._dbCase(as[0], quote)} as ${this._dbCase(as[1])}`;
		}).concat(this._paginated ? `count(${this._table}.*) over() as total` : []).join(', ');
	}

	_formatParameter(keyPath, value) {
		let [table, key] = keyPath.split('.');
		if (typeof key === 'undefined') [table, key] = [this._table, table];
		switch ((tableInformation[caseit(table, this._options.casing.db)] || {})[caseit(key, this._options.casing.db)]) {
		case 'jsonb':
			return typeof value === 'string' ? value : JSON.stringify(value);
		default:
			return value;
		}
	}

	get _operatorMap() {
		return {
			'$or': 'or',
			'$and': 'and'
		};
	}

	get _comparerMap() {
		return {
			$eq: '=',
			$ne: '!=',
			$neq: '!=',
			$lt: '<',
			$lte: '<=',
			$gt: '>',
			$gte: '>=',
			$regexp: '~*',
			$jsonContains: '@>',
			$jsonNotContains: '@>',
			$jsonArrayContains: '?'
		};
	}

	get _comparerPrefixMap() {
		return {
			$jsonNotContains: 'not'
		};
	}

	_buildCondition(lhs, comparer, rhs) {
		const casedComparer = caseit(comparer);
		const condition = `${lhs} ${this._comparerMap[casedComparer]} ${rhs}`;
		const prefix = this._comparerPrefixMap[casedComparer];
		if (!prefix) return condition;
		return `${prefix} (${condition})`;
	}

	_buildConditions(conditions, operator = '$and', comparer = '$eq', wrap = true) {

		if (!conditions) throw new TypeError('No conditions provided.');

		const result = conditions.map((condition) => {

			let key = Object.keys(condition)[0];

			if (key.substring(0, 1) == '$') {
				switch (caseit(key)) {
				case '$or':
				case '$and':
					return this._buildConditions(condition[key], key, comparer, true);
				case '$eq':
				case '$ne':
				case '$neq':
				case '$lt':
				case '$lte':
				case '$gt':
				case '$gte':
				case '$regexp':
				case '$jsonContains':
				case '$jsonNotContains':
				case '$jsonArrayContains':
					return this._buildConditions(condition[key], operator, key, true);
				default:
					throw new TypeError(`Unknown modifier ${caseit(key)}.`);
				}
			}

			if (key.substring(0, 1) == ':') {
				this._queryParameters.push(this._formatParameter(key, condition[key]));
				return this._buildCondition(key.substring(1), comparer, `$${this._queryParameters.length}`);
			}

			let dbKey = key;

			if (dbKey.indexOf('.') == -1) {
				if (dbKey.substring(0, 1) === '!') {
					dbKey = dbKey.substring(1);
				} else {
					dbKey = dbKey
						.split('->')
						.map((part) => `"${part}"`)
						.join('->');
				}
			}

			if (condition[key] == null) {
				switch (comparer) {
				case '$eq':
					return `${dbKey} is null`;
				case '$neq':
					// fallthrough
				case '$ne':
					return `${dbKey} is not null`;
				default:
					throw new TypeError(`Modifier ${comparer} is not usable with \`null\` values.`);
				}
			}

			if (!Array.isArray(condition[key])) {
				this._queryParameters.push(this._formatParameter(key, condition[key]));
				return this._buildCondition(dbKey, comparer, `$${this._queryParameters.length}`);
			} else {
				const values = condition[key].map((value) => {
					this._queryParameters.push(this._formatParameter(key, value));
					return `$${this._queryParameters.length}`;
				});
				return this._buildCondition(dbKey, comparer, `any(array[${values.join(',')}])`);
			}

		}).filter((part) => part.length).join(` ${this._operatorMap[operator]} `);
		if (wrap && result.length) return `(${result})`;
		return result;

	}

	_buildWhere(conditions, statement = 'where') {
		conditions = conditions || this._conditions;
		if (!conditions.length) return;
		const result = this._buildConditions(conditions);
		if (!result.length) return '';
		return `${statement} ${result}`;
	}

	_buildHaving() {
		return this._buildWhere(this._having, 'having');
	}

	_buildJoins() {
		return this._joins.map((join) => {
			if (join.conditions) {
				let type;
				switch (join.required) {
				case 'both': type = 'join'; break;
				case 'local': type = 'left join'; break;
				case 'foreign': type = 'right join'; break;
				case 'none': type = 'outer join'; break;
				}
				return `${type} ${this._dbCase(join.table)} on ${this._buildConditions(join.conditions)}`;
			} else {
				return `cross join ${this._dbCase(join.table)}`;
			}
		}).join(' ');
	}

	_buildSorting() {

		if (!this._sortingKeys.length) return;

		const escapeIfNeeded = (value) => {
			if (value.substring(0, 1) == ':') return value.substring(1);
			return this._dbCase(value, true);
		};

		return `order by ${this._sortingKeys.map((sortingKey) => {
			let condition = escapeIfNeeded(sortingKey.key);
			if (Array.isArray(sortingKey.values)) {
				condition = `case ${this._dbCase(sortingKey.key)} ${sortingKey.values.map((value, idx) => {
					return `when '${value}' then ${idx}`;
				}).join(' ')} end`;
			}
			return `${condition}${sortingKey.order === 'desc' ? ' desc' : ''}`;
		}).join(', ')}`;

	}

	_buildOffset() {
		if (!this._offset) return;
		return `offset ${this._offset}`;
	}

	_buildLimit() {
		if (typeof this._limit === 'undefined') return;
		return `limit ${this._limit}`;
	}

	_buildUpdateKeysAndValues(keys, values) {
		return `set ${keys.map((key, idx) => {
			let value = values[idx];
			if (value == null) {
				value = 'null';
			} else if (/^:/.test(value)) {
				value = value.substring(1);
			} else {
				this._queryParameters.push(this._formatParameter(key, value));
				value = `$${this._queryParameters.length}`;
			}
			return `${this._dbCase(key, true)} = ${value}`;
		}).join(', ')}`;
	}

	_buildUpdate(keys, values) {
		return this._buildUpdateKeysAndValues(keys || this._updateKeys, values || this._updateValues);
	}

	_buildInsertValues() {
		return this._insertValues.map((value, idx) => {
			this._queryParameters.push(this._formatParameter(this._insertKeys[idx], value));
			return `$${this._queryParameters.length}`;
		}).join(', ');
	}

	_buildInsert() {
		if (!Object.keys(this._insertKeys).length) return 'default values';
		return `(${this._buildKeys(this._insertKeys, true)}) values (${this._buildInsertValues()})`;
	}

	_buildGroup() {
		if (!this._groupBy) return '';
		return `group by ${this._dbCase(this._groupBy)}`;
	}

	_buildOnConflict() {
		if (!this._onConflict) return '';
		let result = `on conflict (${this._buildKeys(this._onConflict.keys, true)}) do `;
		switch (Object.keys(this._onConflict.action || {})[0] || 'nothing') {
		case 'nothing':
			result += 'nothing';
			break;
		case 'update':
			result += `update ${this._buildUpdateKeysAndValues(this._onConflict.action.update.keys, this._onConflict.action.update.values)}`;
			break;
		default:
			break;
		}
		return result;
	}

	_build() {

		this._queryParameters = [];

		const command = this._command || 'select';

		let parts = [command];

		switch (command) {
		case 'select':
			parts = parts.concat([
				this._buildKeys(this._selectKeys, true),
				'from',
				this._table,
				this._buildJoins(),
				this._buildWhere(),
				this._buildGroup(),
				this._buildHaving(),
				this._buildSorting(),
				this._buildOffset(),
				this._buildLimit()
			]);
			break;
		case 'update':
			parts = parts.concat([
				this._table,
				this._buildUpdate(),
				this._buildWhere(),
				'returning',
				this._buildKeys(this._selectKeys, true)
			]);
			break;
		case 'insert':
			parts = parts.concat([
				'into',
				this._table,
				this._buildInsert(),
				this._buildOnConflict(),
				'returning',
				this._buildKeys(this._selectKeys, true)
			]);
			break;
		case 'delete':
			parts = parts.concat([
				'from',
				this._table,
				this._buildWhere()
			]);
			break;
		}

		return [parts.filter((part) => part && part.length).join(' '), this._queryParameters];

	}

	async _resolveTableInformation() {
		await tableInformationQueue.add(async () => {

			const tables = [this._table]
				.concat(this._joins.map((join) => join.table))
				.filter((table) => !Object.keys(tableInformation).includes(table));

			await Promise.all(tables.map(async (table) => {
				const rows = await this._connection.exec(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '${table}';`, [], { format: 'raw' });
				Object.assign(tableInformation, Object.fromEntries([[table, Object.fromEntries(rows.map((row) => {
					return [row.column_name, row.data_type];
				}))]]));
			}));

		});
	}

	async _exec() {

		await this._resolveTableInformation();

		const [query, parameters] = this._build();

		let rows = await this._connection.exec(
			query,
			parameters,
			{
				first: this._first,
				transaction: this._transaction
			});

		if (['null', 'undefined'].includes(typeof rows)) {
			rows = this._defaultResult;
		}

		if (this._paginated && !this._first) {
			let total;
			if (rows.length == 0) {
				delete this._paginated;
				delete this._offset;
				delete this._limit;
				this._sortingKeys = [];
				total = parseInt(await this.count('*')._exec());
			} else {
				total = parseInt(((rows || [])[0] || {})['total'] || 0);
			}
			rows.forEach((item) => delete item.total);
			return { total, items: rows };
		}

		return rows;

	}

	then(resolve, reject) {
		super.then(resolve, reject);
		this._exec()
			.then((...args) => this._resolve(...args))
			.catch((error) => this._reject(error));
	}

}