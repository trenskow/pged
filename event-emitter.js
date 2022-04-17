'use strict';

module.exports = exports = class EventEmitter {

	constructor() {
		this._identifier = -1;
		this._listeners = {};
	}

	on(name, handler) {
		this._listeners[name] = this._listeners[name] || [];
		this._listeners[name].push({
			handler,
			identifier: ++this._identifier
		});
		return this._identifier;
	}

	remove(name, identifier) {
		if (typeof this._listeners[name] === 'undefined') return;
		this._listeners[name] = this._listeners[name].filter((listener) => {
			return listener.handler !== identifier && listener.identifier !== identifier;
		});
	}

	once(name, handler) {
		const identifier = this.on(name, async (...args) => {
			this.remove(name, identifier);
			await handler(...args);
		});
	}

	removeAll(name) {
		this._identifier[name] = undefined;
	}

	async emit(name, ...args) {
		await Promise.all((this._listeners[name] || []).map(async (listener) => {
			await listener.handler(...args);
		}));
	}

};
