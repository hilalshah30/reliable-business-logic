"""Illustrative idempotent capacity reservations backed by SQLite."""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Iterator


class InvalidRequest(ValueError):
    """Raised when a reservation request violates its input contract."""


class IdempotencyConflict(ValueError):
    """Raised when a request key is reused with a different payload."""


class UnknownResource(LookupError):
    """Raised when a reservation references an unknown resource."""


class InsufficientCapacity(ValueError):
    """Raised when a resource cannot satisfy the requested reservation."""


@dataclass(frozen=True)
class ReservationRequest:
    resource_id: str
    request_key: str
    units: int


@dataclass(frozen=True)
class Reservation:
    reservation_id: int
    resource_id: str
    request_key: str
    units: int
    created: bool


def validate_request(request: ReservationRequest) -> ReservationRequest:
    """Validate, but do not mutate, a reservation request."""
    if not isinstance(request, ReservationRequest):
        raise InvalidRequest("A ReservationRequest is required.")

    for name, value in (
        ("resource_id", request.resource_id),
        ("request_key", request.request_key),
    ):
        if not isinstance(value, str):
            raise InvalidRequest(f"{name} must be text.")
        if not value or value != value.strip():
            raise InvalidRequest(f"{name} must be non-empty and trimmed.")
        if len(value) > 80:
            raise InvalidRequest(f"{name} must not exceed 80 characters.")

    if type(request.units) is not int or request.units <= 0:
        raise InvalidRequest("units must be a positive integer.")
    return request


class SQLiteReservationStore:
    """Persistence adapter using one SQLite transaction per write."""

    def __init__(self, database_path: str | Path, timeout: float = 10.0):
        self._database_path = str(database_path)
        if timeout <= 0:
            raise ValueError("timeout must be positive.")
        self._timeout = timeout

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database_path,
            timeout=self._timeout,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        primary_error = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException as error:
            primary_error = error
            try:
                connection.rollback()
            except BaseException as cleanup_error:
                raise error from cleanup_error
            raise
        finally:
            try:
                connection.close()
            except BaseException as close_error:
                if primary_error is None:
                    raise
                raise primary_error from close_error

    def initialize(self) -> None:
        """Create the sample schema; callers choose the database location."""
        connection = self._connect()
        try:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS resources (
                    resource_id TEXT PRIMARY KEY,
                    remaining_units INTEGER NOT NULL CHECK (remaining_units >= 0)
                );
                CREATE TABLE IF NOT EXISTS reservations (
                    reservation_id INTEGER PRIMARY KEY,
                    resource_id TEXT NOT NULL REFERENCES resources(resource_id),
                    request_key TEXT NOT NULL,
                    units INTEGER NOT NULL CHECK (units > 0),
                    UNIQUE (resource_id, request_key)
                );
                """
            )
        finally:
            connection.close()

    def add_resource(self, resource_id: str, capacity: int) -> None:
        if (
            not isinstance(resource_id, str)
            or not resource_id
            or resource_id != resource_id.strip()
            or len(resource_id) > 80
        ):
            raise InvalidRequest("resource_id must be trimmed text of at most 80 characters.")
        if type(capacity) is not int or capacity < 0:
            raise InvalidRequest("capacity must be a non-negative integer.")

        with self._write_transaction() as connection:
            connection.execute(
                "INSERT INTO resources (resource_id, remaining_units) VALUES (?, ?)",
                (resource_id, capacity),
            )

    def reserve(self, request: ReservationRequest) -> Reservation:
        request = validate_request(request)
        with self._write_transaction() as connection:
            existing = connection.execute(
                """
                SELECT reservation_id, units
                FROM reservations
                WHERE resource_id = ? AND request_key = ?
                """,
                (request.resource_id, request.request_key),
            ).fetchone()
            if existing is not None:
                if existing["units"] != request.units:
                    raise IdempotencyConflict(
                        "This request key was already used with a different payload."
                    )
                return Reservation(
                    reservation_id=existing["reservation_id"],
                    resource_id=request.resource_id,
                    request_key=request.request_key,
                    units=existing["units"],
                    created=False,
                )

            update = connection.execute(
                """
                UPDATE resources
                SET remaining_units = remaining_units - ?
                WHERE resource_id = ? AND remaining_units >= ?
                """,
                (request.units, request.resource_id, request.units),
            )
            if update.rowcount != 1:
                resource = connection.execute(
                    "SELECT 1 FROM resources WHERE resource_id = ?",
                    (request.resource_id,),
                ).fetchone()
                if resource is None:
                    raise UnknownResource("The requested resource does not exist.")
                raise InsufficientCapacity("The resource has insufficient capacity.")

            inserted = connection.execute(
                """
                INSERT INTO reservations (resource_id, request_key, units)
                VALUES (?, ?, ?)
                """,
                (request.resource_id, request.request_key, request.units),
            )
            return Reservation(
                reservation_id=inserted.lastrowid,
                resource_id=request.resource_id,
                request_key=request.request_key,
                units=request.units,
                created=True,
            )

    def remaining_capacity(self, resource_id: str) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT remaining_units FROM resources WHERE resource_id = ?",
                (resource_id,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise UnknownResource("The requested resource does not exist.")
        return row["remaining_units"]

    def reservation_count(self, resource_id: str) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT COUNT(*) FROM reservations WHERE resource_id = ?",
                (resource_id,),
            ).fetchone()
        finally:
            connection.close()
        return row[0]


class ReservationService:
    """Small domain facade; persistence and transaction policy stay in the store."""

    def __init__(self, store: SQLiteReservationStore):
        self._store = store

    def reserve(self, request: ReservationRequest) -> Reservation:
        return self._store.reserve(validate_request(request))
