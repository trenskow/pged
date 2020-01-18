@trenskow/pged
----

Just a silly little db management and query builder for Postgresql.

# Usage

    const PGed = require('@trenskow/pged');
    
    const db = new PGed({ /* options (see below) */});
    
    const updatedUser = await db.transaction(async () => {
    
        await db
            .from('users')
            .where({ id: 12 })
            .update({ username: 'myusername' });
        
        return await db
            .from('users')
            .select('id,username')
            .where({
                $or: {
                    id: 12,
                    username: 'myusername'
                }
            })
            .first();
    
    });

In the above example we wrap our operations in a transaction, which automatically triggers connection to the Postgresql server if not present. The transaction is automatically commited if no error occurs, and automatically rolled back if an error does occur.

Transactions can be inside transactions - the library will figure out when to commit or roll back.

## Options

These options are supported when creating a new `PGed` instance.

| Name        | Type         | Description | Values | Default    |
|:------------|:------------:|:------------|:-------|:-----------|
| `casing`    | `Object`     | See below
| `casing.db` | `String`     | The casing to use in the db. | Any supported by the [caseit](https://www.npmjs.com/package/@trenskow/caseit) package. | `snake` |
| `casing.js` | `String`     | The casing to use in js. | Same as above | `camel`

### Postgres connection

To set connection parameters use environment variables or do as below.

    const PGed = require('@trenskow/pged');

    PGed.pg = { /* Options */ };

    const db = new PGed({ /* options */ })

See the [pg](https://www.npmjs.com/package/pg) package for available environment variables and options.

# License

See LICENSE.
