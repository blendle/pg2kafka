pg2kafka
--------

Send snapshots and create/update/delete events in Postgres to Kafka.


## Usage

Connect pg2kafka to the database you want to stream changes from, and set the
`PERFORM_MIGRATIONS` env var to `true`, this will create a schema `pg2kafka` in
said DB and will set up an `outbound_event_queue` table there, together with the
necessary functions and triggers to start exporting data.

In you application, create a migration that sets up a given table for exporting,
we need to pass it a table name and an external ID. The external ID will be
what's used as a partitioning key in Kafka, this ensures that messages for a
given entity will always end up in order, on the same partition.

```sql
SELECT pg2kafka.setup('users', 'uuid')
```

Now once you start making changes to your table, you should start seeing events
come in on the `pg2kafka.core_api_production.users` topic:

```sql
UPDATE users SET name = 'bartos' WHERE name = 'bart';
```

```json
{
  "uuid": "b3a5460a-60ad-4f88-b628-1cb57a11de53",
  "external_id": "e5a30fc0-83b1-4203-ac4a-a07f873e1acf",
  "statement": "SNAPSHOT",
  "data": {
    "name": "bart",
    "uuid": "3798821b-446a-4ca0-8524-989d266dfefe",
    "email": "bart@example.com"
  },
  "created_at": "2017-10-30T10:42:40.334213Z"
}
{
  "uuid": "e5a30fc0-83b1-4203-ac4a-a07f873e1acf",
  "external_id": "3798821b-446a-4ca0-8524-989d266dfefe",
  "statement": "UPDATE",
  "data": {
    "name": "bartos"
  },
  "created_at": "2017-10-30T10:42:54.342913Z"
}
```

The producer topics are all in the form of
`pg2kafka.$database_name.$table_name`, you need to make sure that this topic
exists, or else pg2kafka will crash.
