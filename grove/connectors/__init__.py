"""Grove connectors."""

import abc
import datetime
import hashlib
import json
import logging
import os
from typing import Any, Dict, List, Optional

import jmespath

from grove.constants import (
    CACHE_KEY_LOCK,
    CACHE_KEY_POINTER,
    CACHE_KEY_POINTER_NEXT,
    CACHE_KEY_POINTER_PREV,
    CACHE_KEY_SEEN,
    CACHE_KEY_WINDOW_END,
    CACHE_KEY_WINDOW_START,
    CHRONOLOGICAL,
    DATESTAMP_FORMAT,
    DEFAULT_CACHE_HANDLER,
    DEFAULT_LOCK_DURATION,
    DEFAULT_OUTPUT_HANDLER,
    ENV_GROVE_CACHE_HANDLER,
    ENV_GROVE_LOCK_DURATION,
    ENV_GROVE_OUTPUT_HANDLER,
    LOCK_DATE_FORMAT,
    PLUGIN_GROUP_CACHE,
    PLUGIN_GROUP_OUTPUT,
    REVERSE_CHRONOLOGICAL,
)
from grove.exceptions import (
    AccessException,
    ConcurrencyException,
    DataFormatException,
    GroveException,
    NotFoundException,
)
from grove.helpers import plugin
from grove.models import ConnectorConfig


class BaseConnector:
    NAME = "base"
    POINTER_PATH = "NOT_SET"
    LOG_ORDER = REVERSE_CHRONOLOGICAL

    def __init__(self, config: ConnectorConfig, context: Dict[str, str]):
        """Sets up a Grove connector.

        :param config: A valid ConnectorConfig object containing information to use
            when configuring this connector.
        :param context: Contextual information relating to the current runtime.
        """
        self.logger = logging.getLogger(__name__)
        self.configuration = config
        self.runtime_context = context

        # 'Operations' are useful for APIs which have MANY different types of data which
        # can be returned but where only a small subset are pertinent. In this case, the
        # operation can be set in the configuration to instruct grove to only collect
        # those types of log data.
        self.key = self.configuration.key
        self.identity = self.configuration.identity
        self.operation = self.configuration.operation

        # Define contextual log data to be appended to all log messages.
        self.log_context = {
            "operation": self.operation,
            "connector": self.__class__.__module__,
            "identity": self.identity,
        }

        # Let the caller handle exceptions from failure to load handlers directly.
        self._output = plugin.load_handler(
            os.environ.get(ENV_GROVE_OUTPUT_HANDLER, DEFAULT_OUTPUT_HANDLER),
            PLUGIN_GROUP_OUTPUT,
        )
        self._cache = plugin.load_handler(
            os.environ.get(ENV_GROVE_CACHE_HANDLER, DEFAULT_CACHE_HANDLER),
            PLUGIN_GROUP_CACHE,
        )

        # The time that our current lock expires, if we have one.
        self._lock_expiry: Optional[datetime.datetime] = None

        try:
            self._lock_duration = int(
                os.environ.get(ENV_GROVE_LOCK_DURATION, DEFAULT_LOCK_DURATION)
            )
        except ValueError as err:
            self.logger.warning(
                f"Lock duration ('{ENV_GROVE_LOCK_DURATION}') must be a number.",
                extra={"exception": err},
            )

        # Determines if the start of a 'window' has been passed during a collection.
        # This is used to track windows which span multiple pages of results and is only
        # applicable for logs collected in reverse chronological order.
        self._window_passed = False
        self._window_start = str()
        self._window_end = str()

        # Tracks the total number of saved log entries.
        self._saved = 0

        # Paginated / chunked data needs an incrementing identifier to keep things
        # orderly.
        self._part = 0

        # Tracks hashes of unique log entries, keyed by their pointer value.
        self._hashes: Dict[str, set[str]] = {}

        # Pointers track the last collected record in order for collection to continue
        # at the correct place between runs. A "next" pointer is only applicable for
        # logs collected in reverse chronological order.
        self._pointer = str()
        self._pointer_next = str()
        self._pointer_previous = str()

    def run(self):
        """Connector entrypoint, called by the scheduler.

        Wraps collect to handle exceptions and logging for collection. This method
        should NOT be implemented by connectors as it is only intended to provide a
        consistent calling and error handling mechanism when connectors are executed.
        """
        # Acquire a lock first.
        try:
            self.lock()
        except ConcurrencyException as err:
            self.logger.warning(
                "Connector may already be running in another location.",
                extra={"exception": err, **self.log_context},
            )

        # Perform collection.
        try:
            self.collect()
        except GroveException as err:
            self.logger.error(
                "Connector was unable to collect logs.",
                extra={"exception": err, **self.log_context},
            )
            self.unlock()
            return

        # Call the correct post run method.
        if self.LOG_ORDER == CHRONOLOGICAL:
            self._run_chronological()

        if self.LOG_ORDER == REVERSE_CHRONOLOGICAL:
            self._run_reverse_chronological()

        # TODO: The use of a context manager for lock management would be best.
        self.unlock()

    def _run_chronological(self):
        """Performs chronological specific post collection operations."""
        try:
            self.logger.debug(
                "Saving deduplication hashes to cache",
                extra=self.log_context,
            )
            self.save_hashes()
        except AccessException as err:
            self.logger.error(
                "Failed to save hashes to cache.",
                extra={"exception": err, **self.log_context},
            )
            return

    def _run_reverse_chronological(self):
        """Performs reverse chronological specific post collection operations."""
        # Swap pointers.
        try:
            self.pointer = self.pointer_next
            self.logger.debug(
                "Pointer successfully saved to cache.",
                extra={"pointer": self.pointer_next, **self.log_context},
            )
        except AccessException as err:
            self.logger.error(
                "Connector failed to save pointer, cannot continue.",
                extra={"exception": err, **self.log_context},
            )
            return

        # If the collection complete without error delete the window locations and
        # the next pointer from cache, if set.
        try:
            self.logger.debug(
                "Deleting window and next pointer after successful collection.",
                extra=self.log_context,
            )
            self._cache.delete(
                self.cache_key(CACHE_KEY_WINDOW_START),
                self.operation,
            )
            self._cache.delete(
                self.cache_key(CACHE_KEY_WINDOW_END),
                self.operation,
            )
            self._cache.delete(
                self.cache_key(CACHE_KEY_POINTER_NEXT),
                self.operation,
            )
        except AccessException as err:
            self.logger.error(
                "Failed to clean up windows and next pointer from cache.",
                extra={"exception": err, **self.log_context},
            )
            return

        try:
            self.logger.debug(
                "Saving deduplication hashes to cache",
                extra=self.log_context,
            )
            self.save_hashes()
        except AccessException as err:
            self.logger.error(
                "Failed to save hashes to cache.",
                extra={"exception": err, **self.log_context},
            )
            return

    @abc.abstractmethod
    def collect(self):
        """Provides a stub for a connector to initiate a collection."""
        pass

    def save(self, candidates: List[Any]):
        """Saves log candidates, and updates the pointer in the cache.

        :param candidates: List of log candidates to save.

        :raises GroveException: The LOG_ORDER defined by the Connector is not valid.

        :return: A count of entries saved.
        """
        entries = self.deduplicate(candidates)

        if len(entries) < 1:
            self.logger.warning(
                "No log entries passed to save, skipping.", extra=self.log_context
            )
            return

        # Always refresh our lock while saving. This allows us to grab a new lock for
        # every page of data to try and prevent our lock expiring before we've performed
        # a full collection.
        self.lock()

        if self.LOG_ORDER == CHRONOLOGICAL:
            return self._save_chronological(entries)

        if self.LOG_ORDER == REVERSE_CHRONOLOGICAL:
            return self._save_reverse_chronological(entries)

        # Fall through for anything not supported / incorrectly specified.
        raise GroveException(f"Connector LOG_ORDER '{self.LOG_ORDER}' is not valid.")

    def _save_chronological(self, candidates: List[Any]):
        """Saves log entries when retrieved logs are in chronological order.

        :param candidates: List of log entries to save.
        """
        newest = jmespath.search(self.POINTER_PATH, candidates[-1])

        if newest is None:
            self.logger.error(
                "Pointer path was not found in returned logs, cannot continue.",
                extra={"pointer_path": self.POINTER_PATH, **self.log_context},
            )
            return

        # Generate metadata for the candidate log entries, and save to the output
        # handler.
        try:
            self._output.submit(
                data=self._output.serialize(
                    data=candidates,
                    metadata=self.metadata(),
                ),
                part=self._part,
                operation=self.operation,
                connector=self.NAME,
                identity=self.identity,
            )
            self.logger.info(
                "Log submitted successfully to output.",
                extra={"part": self._part, **self.log_context},
            )
        except AccessException as err:
            self.logger.error(
                "Failed to write logs to output, cannot continue.",
                extra={"exception": err, **self.log_context},
            )
            return

        # Once uploaded, then update the pointer. NOTE: There is an opportunity for
        # issues to occur between the output and pointer update which would lead to
        # duplicate data.
        try:
            self.pointer = newest
            self.logger.info(
                "Pointer successfully saved to cache.",
                extra={"pointer": newest, **self.log_context},
            )
        except AccessException as err:
            self.logger.error(
                "Failed to save pointer to cache, cannot continue.",
                extra={"exception": err, **self.log_context},
            )
            return

        # Get ready for the next block of candidate log entries (if required).
        self._part += 1
        self._saved += len(candidates)

    def _save_reverse_chronological(self, candidates: List[Any]):  # noqa: C901
        """Save log entries when logs are in reverse chronological order.

        Data returned in reverse chronological order is more complicated to handle,
        as the data may be paginated, and the execution environment "unreliable". This
        results in more operations are against the cache.

        TODO: This method needs some love. It's very complex, and could do with either
        simplifying or breaking into a number of methods.

        :param candidates: List of log entries to save.
        """
        entries = []
        oldest = jmespath.search(self.POINTER_PATH, candidates[-1])
        newest = jmespath.search(self.POINTER_PATH, candidates[0])

        if oldest is None or newest is None:
            self.logger.error(
                "Pointer path was not found in logs entry, cannot continue.",
                extra={"pointer_path": self.POINTER_PATH, **self.log_context},
            )
            return

        # If a window start is in the cache then a previous collection is incomplete.
        # We'll skip entries until we find our window, and then only collect entries
        # which are ON AND AFTER the end of the window. Any skipped entries will be
        # fetched by subsequent collections.
        incomplete_collection = False

        try:
            if self.window_start:
                incomplete_collection = True
        except NotFoundException:
            pass

        if incomplete_collection:
            for entry in candidates:
                current_pointer = jmespath.search(self.POINTER_PATH, entry)

                if current_pointer is None:
                    self.logger.error(
                        "Pointer path was not found in logs entry, cannot continue.",
                        extra={"pointer_path": self.POINTER_PATH, **self.log_context},
                    )
                    return

                # We need to track FROM the window end, inclusive, to ensure that we
                # don't miss any logs. This is required in cases where the timestamp
                # granularity from the upstream API is only seconds (as always, we
                # prefer duplicates to missing logs).
                #
                # TODO: There is a condition here were duplicates are not considered.
                if not self._window_passed and str(current_pointer) == self.window_end:
                    self._window_passed = True

                # This is expensive, as every log entry will result in a write to the
                # cache. This is why reverse chronological ordering should be avoided
                # where possible.
                if self._window_passed:
                    entries.append(entry)
                    self.window_end = current_pointer

        # Explicit test here rather than 'else' as the previous block is a little long.
        if not incomplete_collection:
            # Save all data if we don't have an existing incomplete collection.
            for entry in candidates:
                entries.append(entry)

            # Track the window of this collection, and the next pointer.
            self._window_passed = True

            self.window_start = newest
            self.window_end = oldest
            self.pointer_next = newest

        if len(entries) < 1:
            return

        # Generate metadata for the entries, and save to the output handler.
        try:
            self._output.submit(
                data=self._output.serialize(
                    data=entries,
                    metadata=self.metadata(),
                ),
                part=self._part,
                operation=self.operation,
                connector=self.NAME,
                identity=self.identity,
            )
            self.logger.info(
                "Log submitted successfully to output.",
                extra={"part": self._part, **self.log_context},
            )
        except AccessException as err:
            self.logger.error(
                "Failed to write logs to output, cannot continue.",
                extra={"exception": err, **self.log_context},
            )
            return

        # Get ready for the next block of entries (if required).
        self._part += 1
        self._saved += len(entries)

        # Save the new window geometry to cache but only AFTER data is saved, and only
        # save the window start when it's updated.
        if not incomplete_collection:
            self.save_window_start()

        self.save_window_end()

    def metadata(self) -> Dict[str, Any]:
        """Returns contextual metadata associated with this collection.

        :return: A dictionary of metadata for storing with log entries.
        """
        return {
            "connector": self.__class__.__module__,
            "identity": self.identity,
            "operation": self.operation,
            "pointer": self.pointer,
            "previous_pointer": self.pointer_previous,
            "collection_time": datetime.datetime.utcnow().strftime(DATESTAMP_FORMAT),
            "runtime": self.runtime_context,
        }

    def cache_key(self, prefix: str = CACHE_KEY_POINTER) -> str:
        """Generates a cache key which uniquely identifies this connector.

        This includes the name and identity to allow multiple instances of the same
        connector to be used concurrently.

        :param prefix: A prefix for the cache key.

        :return: The constructed cache key.
        """
        # MD5 may not be cryptographically secure, but it works for our purposes. It's:
        #
        #   1) Short.
        #   2) Has a low chance of (non-deliberate) collisions.
        #   3) Able to be 'stringified' as hex, the character set of which is compatible
        #      with backends like DynamoDB.
        #
        return ".".join(
            [
                prefix,
                self.NAME,
                hashlib.md5(bytes(self.identity, "utf-8")).hexdigest(),  # noqa: S324
            ]
        )

    def hash_entry(self, entry: Any) -> str:
        """Serialise and hash the provided log entry.

        This is intended to produce a unique identifier for this event which may be used
        for deduplication.

        :param entry: The log entry to hash.

        :return: A hash of a log entry.
        """
        content = bytes(json.dumps(entry, separators=(",", ":")), "utf-8")

        return hashlib.md5(content).hexdigest()

    def hash_entries(self, entries: List[Any]) -> Dict[str, set[str]]:
        """Hashes a list of log entries.

        :param entries: List of log entries to hash

        :return: A dictionary containing a set of log hashes, keyed by the pointer of
            each event.
        """
        hashes: Dict[str, set[str]] = {}

        for entry in entries:
            # If we can't find a pointer in the log entry, just skip it.
            entry_pointer = jmespath.search(self.POINTER_PATH, entry)
            if not entry_pointer:
                continue

            if entry_pointer not in hashes:
                hashes[entry_pointer] = set()

            hashes[entry_pointer].add(self.hash_entry(entry))

        return hashes

    def deduplicate(self, candidates: List[Any]):
        """Deduplicate log entries.

        This is performed by generating a hash of the log entry, and comparing these
        hashes against recently seen events in the cache with the same pointer value.
        If a value is in the cache, then the log entry will be discarded.

        Please note: This only applies to events which have a pointer value that matches
        the most recently saved. This is to prevent needing to keep large amounts of log
        entry hashes.

        :param candidates: A list of log entries to deduplicate.

        :return: A deduplicated list of events.
        """
        entries = []
        old_hashes = self.hashes
        new_hashes: Dict[str, set[str]] = {}

        # Check whether these log entries have been seen already.
        for candidate in candidates:
            current_hash = self.hash_entry(candidate)
            current_pointer = jmespath.search(self.POINTER_PATH, candidate)

            # Track this log entry's hash.
            if current_pointer not in new_hashes:
                new_hashes[current_pointer] = set()

            new_hashes[current_pointer].add(current_hash)

            # Check if the hash for this log entry is in the cache, and if so, skip it.
            try:
                if current_pointer in old_hashes:
                    if current_hash in old_hashes[current_pointer]:
                        continue
            except KeyError:
                pass

            entries.append(candidate)

        # Update known in-memory hashes to include our new entries.
        self.hashes = {**old_hashes, **new_hashes}

        return entries

    @property
    def hashes(self) -> Dict[str, set[str]]:
        """Return hashes for the most recently seen log entries.

        :return: A dictionary of log entry hashes, keyed by their pointer.
        """
        default: Dict[str, set[str]] = {}
        if self._hashes:
            return self._hashes

        try:
            self._hashes[self.pointer] = set(
                json.loads(
                    self._cache.get(self.cache_key(CACHE_KEY_SEEN), self.operation)
                )
            )
        except (TypeError, json.decoder.JSONDecodeError) as err:
            self.logger.warning(
                "Deduplication hashes in the cache appear to be malformed, ignoring.",
                extra={"exception": err, **self.log_context},
            )
            return default
        except NotFoundException:
            return default

        return self._hashes

    @hashes.setter
    def hashes(self, value: Dict[str, set[str]]):
        """Sets recent log entry hashes in memory.

        :param value: A dictionary of sets to save.
        """
        self._hashes = value

    def save_hashes(self):
        """Saves the log entry hashes to cache."""
        serialized = json.dumps(
            list(self._hashes.get(self.pointer, set())), separators=(",", ":")
        )

        self._cache.set(self.cache_key(CACHE_KEY_SEEN), self.operation, serialized)

    @property
    def pointer_previous(self) -> str:
        """Return the previous pointer, fetching from cache if needed.

        :return: The previous pointer, which will be an empty string if no previous
            pointer was found.
        """
        if self._pointer_previous:
            return self._pointer_previous

        try:
            self._pointer_previous = self._cache.get(
                self.cache_key(CACHE_KEY_POINTER_PREV), self.operation
            )
        except NotFoundException:
            self._pointer_previous = str()

        return self._pointer_previous

    @property
    def pointer_next(self) -> str:
        """Return the currently known next pointer, fetching from cache if needed.

        :return: The next pointer associated with this configured connector.
        """
        if self._pointer_next:
            return self._pointer_next

        # Intentionally allow exceptions to bubble up so the caller can catch if the
        # value is not in the cache.
        self._pointer_next = self._cache.get(
            self.cache_key(CACHE_KEY_POINTER_NEXT), self.operation
        )

        return self._pointer_next

    @pointer_next.setter
    def pointer_next(self, value: str):
        """Sets and saves the next pointer in cache.

        :param value: The value to save as the next pointer.
        """
        self._cache.set(
            self.cache_key(CACHE_KEY_POINTER_NEXT), self.operation, str(value)
        )

        # Always swap the in-memory value last.
        self._pointer_next = str(value)

    @property
    def pointer(self) -> str:
        """Return the currently known pointer, fetching from cache if needed.

        :return: Pointer associated with this configured connector.
        """
        if self._pointer:
            return self._pointer

        # Intentionally allow exceptions to bubble up so the caller can catch if the
        # value is not in the cache.
        self._pointer = self._cache.get(
            self.cache_key(CACHE_KEY_POINTER), self.operation
        )

        return self._pointer

    @pointer.setter
    def pointer(self, value: str):
        """Sets and saves the pointer and the previous pointer to cache.

        :param value: The value to save as the pointer.
        """
        new_value = str(value)
        old_value = self._pointer

        self._cache.set(
            self.cache_key(CACHE_KEY_POINTER),
            self.operation,
            new_value,
        )
        self._cache.set(
            self.cache_key(CACHE_KEY_POINTER_PREV),
            self.operation,
            old_value,
        )

        # Always swap the in-memory value last.
        self._pointer = new_value
        self._pointer_previous = old_value

    @property
    def window_start(self) -> str:
        """Return the window start location from cache, if set.

        :return: The window start location, or an empty string if not found.
        """
        if self._window_start:
            return self._window_start

        try:
            self._window_start = self._cache.get(
                self.cache_key(CACHE_KEY_WINDOW_START), self.operation
            )
        except NotFoundException:
            return str()

        return self._window_start

    @window_start.setter
    def window_start(self, value: str):
        """Sets the window start location in memory.

        :param value: The value to save as the window start.
        """
        self._window_start = str(value)

    def save_window_start(self):
        """Saves the window start location to cache."""
        self._cache.set(
            self.cache_key(CACHE_KEY_WINDOW_START), self.operation, self.window_start
        )

    @property
    def window_end(self) -> Optional[str]:
        """Return the window end location from cache, if set.

        :return: The window end location.
        """
        if self._window_end:
            return self._window_end

        try:
            self._window_end = self._cache.get(
                self.cache_key(CACHE_KEY_WINDOW_END), self.operation
            )
        except NotFoundException:
            return None

        return self._window_end

    @window_end.setter
    def window_end(self, value: str):
        """Sets the window end location in memory.

        :param value: The value to save as the window end.
        """
        self._window_end = str(value)

    def save_window_end(self):
        """Saves the window end location to cache."""
        self._cache.set(
            self.cache_key(CACHE_KEY_WINDOW_END), self.operation, self.window_end
        )

    def lock(self):
        """Attempts to acquire an execution lock for the current connector.

        This will raise a ConcurrencyException if a a valid lock is already present.

        :raises ConcurrencyException: A valid lock is already held, likely the result
            of a concurrent execution of Grove.
        """
        now = datetime.datetime.utcnow()
        expiry = now + datetime.timedelta(seconds=self._lock_duration)

        # If we don't have a lock, acquire one.
        current = self._lock_expiry

        if current is None:
            try:
                current = datetime.datetime.strptime(
                    self._cache.get(self.cache_key(CACHE_KEY_LOCK), self.operation),
                    LOCK_DATE_FORMAT,
                )
            except NotFoundException:
                pass

            # Someone else has the lock.
            if current is not None and current >= now:
                raise ConcurrencyException(
                    f"Valid lock already held and does not expire until {current}"
                )

        # No lock, or lock expired lock? Let's grab one for ourselves. If we own the
        # lock, we'll also re-lock with a new expiry to keep it.
        #
        # We're constraining the set here to the value of our existing lock, the value
        # we just got from the cache, or "not set" (None). This is to try and protect
        # against a TOCTTOU condition. That said, this code can be improved.
        not_set = True
        constraint = None

        if current is not None:
            not_set = False
            constraint = current.strftime(LOCK_DATE_FORMAT)

        try:
            self._cache.set(
                self.cache_key(CACHE_KEY_LOCK),
                self.operation,
                expiry.strftime(LOCK_DATE_FORMAT),
                constraint=constraint,
                not_set=not_set,
            )
        except DataFormatException:
            raise ConcurrencyException(
                "Could not acquire lock, a valid lock already exists"
            )

        # Lock is ours, track it.
        self._lock_expiry = expiry

    def unlock(self):
        """Releases an execution lock for the current connector.

        :raises AccessException: An unexpected error occurred while releasing the lock.
        :raises ConcurrencyException: The lock does not match the expected value. This
            may indicate a concurrent execution of Grove has since taken the lock.
        """
        # If we don't have a lock, do nothing.
        if self._lock_expiry is None:
            return

        # If there's no lock set in the cache, do nothing.
        try:
            current = datetime.datetime.strptime(
                self._cache.get(self.cache_key(CACHE_KEY_LOCK), self.operation),
                LOCK_DATE_FORMAT,
            )
        except NotFoundException:
            return

        # Check if the lock matches what we expect.
        if current != self._lock_expiry:
            raise ConcurrencyException(
                "The cached lock does not match ours. Someone else may have taken the "
                f"lock! Abandoning attempt to unlock ({current} vs {self._lock_expiry})"
            )

        # Delete the lock, with a constraint to ensure it hasn't changed hands.
        constraint = current.strftime(LOCK_DATE_FORMAT)

        try:
            self._cache.delete(
                self.cache_key(CACHE_KEY_LOCK),
                self.operation,
                constraint=constraint,
            )
        except DataFormatException:
            raise ConcurrencyException(
                "Could not delete lock, the lock no longer appears to be ours."
            )

        # Bye-bye lock.
        self._lock_expiry = None
