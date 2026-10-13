# Reliable Business Logic: Idempotent Reservations

## Problem

Clients may retry a request when a response is delayed or lost. Without a stable request key, a repeated reservation can consume capacity more than once. Reusing the same key with a changed payload is also ambiguous and must be rejected.

## Design

This standalone sample models fictional capacity reservations. It separates request validation (`ReservationService`) from SQLite persistence (`SQLiteReservationStore`). A request key is unique within a resource. An identical retry returns the original reservation; reusing the key for different units raises `IdempotencyConflict`.

The store uses a SQLite `UNIQUE(resource_id, request_key)` constraint and performs the capacity decrement plus reservation insert inside one `BEGIN IMMEDIATE` transaction. An insert failure rolls the decrement back. The tests exercise retries from multiple threads, each using its own SQLite connection, against a temporary file database. A failed commit can still have an ambiguous outcome if the database committed before reporting an I/O error; callers should query by the same idempotency key before retrying.

## Safety principles

- Validate identifiers and quantities before persistence.
- Use parameterized SQL and database constraints as a final duplicate guard.
- Make repeated requests deterministic and reject changed-payload key reuse.
- Keep the capacity update and reservation insert in one transaction.
- Use isolated temporary databases in tests; no production data or credentials are included.

## Run

Requirements: Python 3.10 or newer. The example uses only the Python standard library.

From this directory, run:

```sh
python -m unittest discover -s tests -v
```

## Coverage

Tests cover a successful reservation, an identical retry, conflicting key reuse, key scope, malformed input, unknown resources, insufficient capacity, rollback after simulated insert and commit failures, cleanup failure chaining, and concurrent retries with both repeated and distinct request keys. The commit-failure test fails before SQLite commits; it cannot simulate power loss or prove durability after an ambiguous real-world commit error.

## Limitations

This is a small illustrative portfolio sample, not the WarehouseWeb production application. It does not implement authentication, external side effects, distributed idempotency, or a general repository abstraction. Its concurrency test demonstrates SQLite's local writer serialization with independent connections in threads; it is not evidence of safety on network filesystems or other database engines. Production systems must select and test a database-appropriate locking and idempotency strategy.
