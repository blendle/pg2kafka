pg2kafka
--------

This service adds triggers to a given table in your Postgres database after
taking a snapshot of it's initial representation, and tracks changes to that
table to deliver them to a Kafka topic.

It consists of two parts:

- A schema in your DB containing an `outbound_event_queue` table and all the
  necessary functions and triggers to take snapshots and track changes.
- A small executable that reads from said table, and ships them to Kafka.

*pg2kafka is still in early development*, it is not advised to use this in
production. If you run into issues, please open an issue.

We use this as a way to reliably get data out of our hosted PostgreSQL databases
where we cannot use systems like [debezium](http://debezium.io) or
[bottled water](https://github.com/confluentinc/bottledwater-pg) since we do not
have access to the WAL logs and cannot install native extensions or run binaries
on the database host machine.

The following SQL statements are used to send updates to the relevant topic:

* `INSERT`
* `UPDATE`
* `DELETE`

## Usage

Connect pg2kafka to the database you want to stream changes from, and set the
`PERFORM_MIGRATIONS` env var to `true`, this will create a schema `pg2kafka` in
said DB and will set up an `outbound_event_queue` table there, together with the
necessary functions and triggers to start exporting data.

In order to start tracking changes for a table, you need to execute the
`pg2kafka.setup` function with the table name and a column to use as external
ID. The external ID will be what's used as a partitioning key in Kafka, this
ensures that messages for a given entity will always end up in order, on the
same partition. The example below will add the trigger to the `products` table
and use its `sku` column as the external ID.

Let's say we have a database called `shop_test`:

```bash
$ createdb shop_test
```

It contains a table `products`:

```sql
CREATE TABLE products (
  id BIGSERIAL,
  sku TEXT,
  name TEXT
);
```

And it already has some data in it:

```sql
INSERT INTO products (sku, name) VALUES ('CM01-R', 'Red Coffee Mug');
INSERT INTO products (sku, name) VALUES ('CM01-B', 'Blue Coffee Mug');
```

Given that we've already connected pg2kafka to it, and it has ran it's
migrations, we can start tracking changes to the `products` table:

```sql
SELECT pg2kafka.setup('products', 'sku');
```

This will create snapshots of the current data in that table:

```json
{
  "uuid": "ea76e080-6acd-413a-96b3-131a42ab1002",
  "external_id": "CM01-B",
  "statement": "SNAPSHOT",
  "data": {
    "id": 2,
    "sku": "CM01-B",
    "name": "Blue Coffee Mug"
  },
  "created_at": "2017-11-02T16:14:36.709116Z"
}
{
  "uuid": "e1c0008d-6b7a-455a-afa6-c1c2eebd65d3",
  "external_id": "CM01-R",
  "statement": "SNAPSHOT",
  "data": {
    "id": 1,
    "sku": "CM01-R",
    "name": "Red Coffee Mug"
  },
  "created_at": "2017-11-02T16:14:36.709116Z"
}
```

Now once you start making changes to your table, you should start seeing events
come in on the `pg2kafka.shop_test.products` topic:

```sql
UPDATE products SET name = 'Big Red Coffee Mug' WHERE sku = 'CM01-R';
```

```json
{
  "uuid": "d6521ce5-4068-45e4-a9ad-c0949033a55b",
  "external_id": "CM01-R",
  "statement": "UPDATE",
  "data": {
    "name": "Big Red Coffee Mug"
  },
  "created_at": "2017-11-02T16:15:13.94077Z"
}
```

The producer topics are all in the form of
`pg2kafka.$database_name.$table_name`, you need to make sure that this topic
exists, or else pg2kafka will crash.

You can optionally prepend a namespace to the Kafka topic, by setting the
`TOPIC_NAMESPACE` environment variable. When doing this, the final topic name
would be `pg2kafka.$namespace.$database_name.$table_name`.

### Cleanup

If you decide not to use pg2kafka anymore you can cleanup the Database triggers
using the following command:

```sql
DROP SCHEMA pg2kafka CASCADE;
```

## Development

### Setup

#### Golang

You will need Go 1.9.

#### PostgreSQL

Set up a database and expose a connection string to it as an env variable, for
local development we also specify `sslmode=disable`.

```bash
$ createdb pg2kafka_test
$ export DATABASE_URL="postgres://user:password@localhost/pg2kafka_test?sslmode=disable"
```

#### Kafka

Install [Kafka](http://kafka.apache.org/) if you don't already have it running.
This is not required to run the tests, but it is required if you want to run
pg2kafka locally against a real Kafka.

Create a topic for the table you want to track in your database:

```bash
kafka-topics \
  --zookeeper localhost:2181 \
  --create \
  --topic pg2kafka.pg2kafka_test.users \
  --replication-factor 1 \
  --partitions 3
```

Then export the Kafka host as an URL so pg2kafka can use it:

```bash
$ export KAFKA_BROKER="localhost:9092"
```

### Running the service locally

Make sure you export the `DATABASE_URL` and `KAFKA_BROKER`, and also
`export PERFORM_MIGRATIONS=true`.

```bash
$ go run main.go
```

To run the service without using Kafka, you can set a `DRY_RUN=true` flag, which
will produce the messages to stdout.

### Running tests

The only thing required for the tests to run is that you've set up a database
and exposed a connection string to it as `DATABASE_URL`. All the necessary
schemas, tables and triggers will be created by the tests.

```bash
$ ./script/test
```

## License
pg2kafka is released under the ISC license. See [LICENSE](https://github.com/blendle/pg2kafka/blob/master/LICENSE) for details.
