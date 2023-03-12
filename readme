## MySQLPlus provides abstractions on top of the mysql2 package.

* Automatic schema generation from javascript objects
* Read, Write and Delete methods
* Data changed event stream
* Audit-trail for all DB changes
* Fine-grain permissions
* Hook to add object defaults
* Qualifiers for security in a multi-tenant environment

## Installation

    npm install @tuilder/mysql-plus

## Usage

```typescript
import { IDbPermissions, MySQLPlus, EDbOperations, ETableChangeType } from '@tuilder/mysql-plus';

const db = new MySQLPlus({
  database: 'test-db',
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  defaults: (schema, table, data) => {
    return {
      ...data,
      _owner: 'fred123',
      _account: accountId,
      _type: table
    }
  }
});

db.eventStream.subscribe(e => {
  console.log(ETableChangeType[e.type], e.table, e.data);
});

const sessionPermissions: IDbPermissions = {
  global: new Set([EDbOperations.Read]),
  tables: {
    person: {
      operations: new Set([EDbOperations.Write]),
      protectedFields: new Set(['password'])
    }
  },
  qualifiers: {
    _account: accountId
  }
}

// Get a connection
const conection = await db.getConnection()

// Write some data
await db.write(permissions, 'person', { id: 'abfui1y2fsbkj', name: 'Emmanuel Clive Higgins', age: 206487, favColor: 'red, yellow and black maybe' })

// Read some data
await db.read(permissions, 'person', { where: { id: 'abfui1y2fsbkj' } })
await db.read(permissions, 'person', { columns: ['id', 'name'] })

// Update some data
await db.write(permissions, 'person', { id: 'abfui1y2fsbkj', age: 30 })

// Delete some data
await db.delete(permissions, 'person', 'abfui1y2fsbkj')

```

Table and field names can be provided in camelCase or snake_case. The library will convert them to snake_case for the database.
