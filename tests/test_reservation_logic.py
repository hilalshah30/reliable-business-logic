from concurrent.futures import ThreadPoolExecutor
import sqlite3
from threading import Barrier
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from reservation_logic import (
    IdempotencyConflict,
    InsufficientCapacity,
    InvalidRequest,
    ReservationRequest,
    ReservationService,
    SQLiteReservationStore,
    UnknownResource,
)


class ReservationServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_directory.name) / "sample.sqlite3"
        self.store = SQLiteReservationStore(self.database_path)
        self.store.initialize()
        self.store.add_resource("fictional-dock-1", 5)
        self.service = ReservationService(self.store)

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_first_valid_request_reserves_capacity(self):
        result = self.service.reserve(
            ReservationRequest("fictional-dock-1", "request-001", 2)
        )

        self.assertTrue(result.created)
        self.assertEqual(result.units, 2)
        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 3)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 1)

    def test_retry_returns_existing_reservation_without_double_decrement(self):
        request = ReservationRequest("fictional-dock-1", "request-002", 2)

        first = self.service.reserve(request)
        retry = self.service.reserve(request)

        self.assertTrue(first.created)
        self.assertFalse(retry.created)
        self.assertEqual(first.reservation_id, retry.reservation_id)
        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 3)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 1)

    def test_same_key_with_different_payload_is_rejected(self):
        self.service.reserve(
            ReservationRequest("fictional-dock-1", "request-003", 1)
        )

        with self.assertRaises(IdempotencyConflict):
            self.service.reserve(
                ReservationRequest("fictional-dock-1", "request-003", 2)
            )

        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 4)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 1)

    def test_idempotency_scope_includes_resource(self):
        self.store.add_resource("fictional-dock-2", 3)

        first = self.service.reserve(
            ReservationRequest("fictional-dock-1", "shared-key", 1)
        )
        second = self.service.reserve(
            ReservationRequest("fictional-dock-2", "shared-key", 1)
        )

        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertNotEqual(first.reservation_id, second.reservation_id)

    def test_invalid_requests_fail_before_changing_capacity(self):
        invalid_requests = (
            ReservationRequest("", "request-004", 1),
            ReservationRequest("fictional-dock-1", " ", 1),
            ReservationRequest(" fictional-dock-1", "request-004", 1),
            ReservationRequest("fictional-dock-1", "request-004", 0),
            ReservationRequest("fictional-dock-1", "request-004", -1),
            ReservationRequest("fictional-dock-1", "request-004", True),
            ReservationRequest("fictional-dock-1", "request-004", 1.5),
        )
        for request in invalid_requests:
            with self.subTest(request=request):
                with self.assertRaises(InvalidRequest):
                    self.service.reserve(request)

        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 5)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 0)

    def test_unknown_resource_and_insufficient_capacity_are_predictable(self):
        with self.assertRaises(UnknownResource):
            self.service.reserve(
                ReservationRequest("missing-dock", "request-005", 1)
            )
        with self.assertRaises(InsufficientCapacity):
            self.service.reserve(
                ReservationRequest("fictional-dock-1", "request-006", 6)
            )

        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 5)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 0)

    def test_insert_failure_rolls_back_capacity_decrement(self):
        connection = sqlite3.connect(self.database_path)
        try:
            connection.execute(
                """
                CREATE TRIGGER reject_reservation_insert
                BEFORE INSERT ON reservations
                BEGIN
                    SELECT RAISE(ABORT, 'simulated persistence failure');
                END;
                """
            )
        finally:
            connection.close()

        with self.assertRaises(sqlite3.IntegrityError):
            self.service.reserve(
                ReservationRequest("fictional-dock-1", "request-007", 2)
            )

        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 5)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 0)

    def test_concurrent_retries_create_one_row_and_decrement_once(self):
        request = ReservationRequest("fictional-dock-1", "request-008", 2)
        with ThreadPoolExecutor(max_workers=8) as executor:
            outcomes = list(executor.map(lambda _index: self.service.reserve(request), range(8)))

        self.assertEqual(sum(outcome.created for outcome in outcomes), 1)
        self.assertEqual(
            len({outcome.reservation_id for outcome in outcomes}),
            1,
        )
        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 3)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 1)

    def test_different_keys_competing_for_capacity_never_over_reserve(self):
        resource_id = "fictional-race-dock"
        initial_capacity = 3
        units_per_request = 2
        self.store.add_resource(resource_id, initial_capacity)
        requests = tuple(
            ReservationRequest(resource_id, f"competition-{index}", units_per_request)
            for index in range(4)
        )
        start_together = Barrier(len(requests))

        def reserve_after_barrier(request):
            start_together.wait(timeout=5)
            try:
                return request, self.service.reserve(request), None
            except InsufficientCapacity as error:
                return request, None, error

        with ThreadPoolExecutor(max_workers=len(requests)) as executor:
            outcomes = list(executor.map(reserve_after_barrier, requests))

        successful = [result for _request, result, error in outcomes if error is None]
        rejected = [request for request, _result, error in outcomes if error is not None]
        reserved_units = sum(result.units for result in successful)
        remaining = self.store.remaining_capacity(resource_id)

        self.assertTrue(successful)
        self.assertTrue(rejected)
        self.assertLessEqual(reserved_units, initial_capacity)
        self.assertGreaterEqual(remaining, 0)
        self.assertEqual(remaining, initial_capacity - reserved_units)
        self.assertEqual(self.store.reservation_count(resource_id), len(successful))

        connection = sqlite3.connect(self.database_path)
        try:
            persisted_keys = {
                row[0]
                for row in connection.execute(
                    "SELECT request_key FROM reservations WHERE resource_id = ?",
                    (resource_id,),
                )
            }
        finally:
            connection.close()
        self.assertEqual(
            persisted_keys,
            {result.request_key for result in successful},
        )
        self.assertTrue(
            all(request.request_key not in persisted_keys for request in rejected)
        )

    def test_commit_failure_rolls_back_and_closes_connection(self):
        connection = self.store._connect()
        failing_connection = _FailingCommitConnection(connection)
        request = ReservationRequest("fictional-dock-1", "commit-failure", 2)

        with patch.object(self.store, "_connect", return_value=failing_connection):
            with self.assertRaisesRegex(
                sqlite3.OperationalError,
                "simulated commit failure",
            ):
                self.service.reserve(request)

        self.assertEqual(failing_connection.rollback_calls, 1)
        self.assertTrue(failing_connection.closed)
        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 5)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 0)

    def test_commit_error_remains_primary_when_rollback_also_fails(self):
        connection = self.store._connect()
        failing_connection = _FailingCommitConnection(
            connection,
            fail_rollback=True,
        )
        request = ReservationRequest("fictional-dock-1", "rollback-failure", 2)

        with patch.object(self.store, "_connect", return_value=failing_connection):
            with self.assertRaisesRegex(
                sqlite3.OperationalError,
                "simulated commit failure",
            ) as raised:
                self.service.reserve(request)

        self.assertIsInstance(raised.exception.__cause__, OSError)
        self.assertEqual(str(raised.exception.__cause__), "simulated rollback failure")
        self.assertEqual(failing_connection.rollback_calls, 1)
        self.assertTrue(failing_connection.closed)
        self.assertEqual(self.store.remaining_capacity("fictional-dock-1"), 5)
        self.assertEqual(self.store.reservation_count("fictional-dock-1"), 0)


class _FailingCommitConnection:
    def __init__(self, connection, *, fail_rollback=False):
        self._connection = connection
        self._fail_rollback = fail_rollback
        self.rollback_calls = 0
        self.closed = False

    def execute(self, *args, **kwargs):
        return self._connection.execute(*args, **kwargs)

    def commit(self):
        raise sqlite3.OperationalError("simulated commit failure")

    def rollback(self):
        self.rollback_calls += 1
        if self._fail_rollback:
            raise OSError("simulated rollback failure")
        return self._connection.rollback()

    def close(self):
        self.closed = True
        return self._connection.close()


if __name__ == "__main__":
    unittest.main()
