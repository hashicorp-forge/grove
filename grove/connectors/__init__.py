# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

"""Grove connectors."""

import abc
import datetime
import hashlib
import json
import logging
import os
from typing import Any, Dict, List, Optional

import jmespath

from grove.__about__ import __version__
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
    PLUGIN_GROUP_PROCESSOR,
    REVERSE_CHRONOLOGICAL,
)
from grove.exceptions import (
    AccessException,
    ConcurrencyException,
    ConfigurationException,
    DataFormatException,
    GroveException,
    NotFoundException,
    ProcessorError,
)
from grove.helpers import parsing, plugin
from grove.models import ConnectorConfig, OutputStream


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
        self.kind = self.__class__.__module__
        self.identity = self.configuration.identity
        self.operation = self.configuration.operation

        # Define contextual log data to be appended to all log messages.
        self.log_context = {
            "operation": self.operation,
            "connector": self.kind,
            "identity": self.identity,
        }

        # Let the caller handle exceptions from failure to load handlers directly.
        self._cache = plugin.load_handler(
            os.environ.get(ENV_GROVE_CACHE_HANDLER, DEFAULT_CACHE_HANDLER),
            PLUGIN_GROUP_CACHE,
        )
        self._output = plugin.load_handler(
            os.environ.get(ENV_GROVE_OUTPUT_HANDLER, DEFAULT_OUTPUT_HANDLER),
            PLUGIN_GROUP_OUTPUT,
        )
        self._output.setup()

        # Processors are only setup once for each connector instance.
        self._processors = {}

        for processor in self.configuration.processors:
            try:
                self._processors[processor.name] = plugin.load_handler(
                    processor.processor,
                    PLUGIN_GROUP_PROCESSOR,
                    processor,
                )
            except ConfigurationException as err:
                raise ProcessorError(
                    f"Failed to initialise processor '{processor.name}' "
                    f"({processor.processor}). {err}",
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
                extra={"exception": err, **self.log_context},
            )

        # Determines if the start of a 'window' has been passed during a collection.
        # This is used to track windows which span multiple pages of results and is only
        # applicable for logs collected in reverse chronological order.
        self._window_passed = False
        self._window_start = str()
        self._window_end = str()

        # Paginated / chunked data needs an incrementing identifier to keep things
        # orderly.
        self._part = 0

        # Track the number of output logs by the configured output destination stream.
        # This allows statistics to be generated on deduplication, splitting, etc.
        self._saved = {}
        for descriptor, _ in self.configuration.outputs.items():
            self._saved[descriptor] = 0

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
                f"Connector '{self.kind}' may already be running in another location.",
                extra={"exception": err, **self.log_context},
            )

        # Perform collection.
        try:
            self.collect()
        except GroveException as err:
            self.logger.error(
                f"Connector '{self.kind}' could not complete collection successfully.",
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
        # TODO: Move to processor.
        try:
            self.logger.debug(
                "Saving deduplication hashes to cache.",
                extra=self.log_context,
            )
            self.save_hashes()
        except AccessException as err:
            self.logger.error(
                f"Connector '{self.kind}' Failed to save hashes to cache.",
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
                f"Connector '{self.kind}' failed to save pointer, cannot continue.",
                extra={"exception": err, **self.log_context},
            )
            return
        except NotFoundException:
            self.logger.debug(
                "Skipping pointer swap and clean-up as there is no next-pointer.",
                extra={**self.log_context},
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
                f"Connector '{self.kind}' failed to clean up windows and next pointer from cache.",  # noqa: E501
                extra={"exception": err, **self.log_context},
            )
            return

        # TODO: Move to processor.
        try:
            self.logger.debug(
                "Saving deduplication hashes to cache",
                extra=self.log_context,
            )
            self.save_hashes()
        except AccessException as err:
            self.logger.error(
                f"Connector '{self.kind}' failed to save hashes to cache.",
                extra={"exception": err, **self.log_context},
            )
            return

    @abc.abstractmethod
    def collect(self):
        """Provides a stub for a connector to initiate a collection."""
        pass

    def process_and_write(self, entries: List[Any]):
        """Write log entries them to the configured output handler.

        :param entries: List of log entries to process.
        """
        # Allow failures to bubble all the way up and fail the run. If processing fails
        # we want to defer collection, to allow retry later. We always pass a copy of
        # the entries to prevent accidental overwriting of the collected raw data by
        # a processor.
        processed = self.process(entries)

        for descriptor, stream in self.configuration.outputs.items():
            # Ensure the output uses the correct stream.
            to_save = entries
            if stream == OutputStream.processed:
                to_save = processed

            number_of_entries = len(to_save)
            if number_of_entries < 1:
                self.logger.info(
                    "No log entries to output for stream, skipping.",
                    extra={
                        "stream": stream,
                        "descriptor": descriptor,
                        **self.log_context,
                    },
                )
                continue

            try:
                self._output.submit(
                    data=self._output.serialize(
                        data=to_save,
                        metadata=self.metadata(),
                    ),
                    part=self._part,
                    operation=self.operation,
                    connector=self.NAME,
                    identity=self.identity,
                    descriptor=descriptor,
                )

                # Update counters.
                self._saved[descriptor] += number_of_entries

                self.logger.info(
                    "Log submitted successfully to output.",
                    extra={
                        "part": self._part,
                        "stream": stream,
                        "descriptor": descriptor,
                        "entries": number_of_entries,
                        **self.log_context,
                    },
                )
            except AccessException as err:
                self.logger.error(
                    f"Connector '{self.kind}' failed to write logs to output, cannot continue.",  # noqa: E501
                    extra={
                        "part": self._part,
                        "exception": err,
                        "stream": stream,
                        "descriptor": descriptor,
                        **self.log_context,
                    },
                )
                raise

    def save(self, entries: List[Any]):
        """Saves log entries, and updates the pointer in the cache.

        :param entries: List of log entries to save.
        """
        # TODO: Move deduplication into a processor.
        entries = self.deduplicate_by_hash(entries)

        if len(entries) < 1:
            self.logger.warning(
                "No log entries to save, skipping.", extra=self.log_context
            )
            return

        # Always refresh our lock while saving. This allows us to grab a new lock for
        # every page of data to try and prevent our lock expiring before we've performed
        # a full collection.
        #
        # Unlock is not called here, as it's performed by the caller.
        self.lock()

        if self.LOG_ORDER == CHRONOLOGICAL:
            self._save_chronological(entries)

        if self.LOG_ORDER == REVERSE_CHRONOLOGICAL:
            self._save_reverse_chronological(entries)

        self.finalize()

    def _save_chronological(self, entries: List[Any]):
        """Saves log entries when retrieved logs are in chronological order.

        :param entries: List of log entries to save.
        """
        # Pointers are extracted prior to processing as processing may modify the
        # structure, or remove entries entirely.
        newest = jmespath.search(self.POINTER_PATH, entries[-1])
        if newest is None:
            raise GroveException(
                f"Pointer path ({self.POINTER_PATH}) was not found in returned logs."
            )

        # Exceptions are allowed to bubble up here to ensure connectors exit on error,
        # rather than silently dropping batches of log entries.
        self.process_and_write(entries)

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
                f"Connector '{self.kind}' failed to save pointer to cache, cannot continue.",  # noqa: E501
                extra={"exception": err, **self.log_context},
            )
            raise

        # Get ready for the next batch of candidate log entries (if required).
        self._part += 1

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
            raise GroveException(
                f"Pointer path ({self.POINTER_PATH}) was not found in returned logs."
            )

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
                    raise GroveException(
                        f"Pointer path ({self.POINTER_PATH}) not found in log entry."
                    )

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

        # Exceptions are allowed to bubble up here to ensure connectors exit on error,
        # rather than silently dropping batches of log entries.
        self.process_and_write(entries)

        # Get ready for the next block of entries (if required).
        self._part += 1

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
            "version": __version__,
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

    def deduplicate_by_hash(self, candidates: List[Any]):
        """Deduplicate log entries by their hash.

        This is performed by generating a hash of the log entry, and comparing these
        hashes against recently seen events in the cache with the same pointer value.
        If a value is in the cache, then the log entry will be discarded.

        Please note: This only applies to events which have a pointer value that matches
        the most recently saved. This is to prevent needing to keep large amounts of log
        entry hashes.

        :param candidates: A list of log entries to deduplicate.

        :return: A deduplicated list of log entries.
        """
        entries = []
        old_hashes = self.hashes
        new_hashes: Dict[str, set[str]] = {}

        # Check whether these log entries have been seen already.
        for candidate in candidates:
            candidate_hash = self.hash_entry(candidate)
            candidate_pointer = str(jmespath.search(self.POINTER_PATH, candidate))

            # Track this log entry's hash.
            if candidate_pointer not in new_hashes:
                new_hashes[candidate_pointer] = set()

            new_hashes[candidate_pointer].add(candidate_hash)

            # Check if the hash for this log entry is in the cache, and if so, skip it.
            try:
                if candidate_pointer in old_hashes:
                    if candidate_hash in old_hashes[candidate_pointer]:
                        continue
            except KeyError:
                pass

            entries.append(candidate)

        # Update known in-memory hashes to include our new entries.
        self.hashes = {**old_hashes, **new_hashes}

        return entries

    def deduplicate_by_pointer(self, entries: List[Any]):
        """Deduplicate log entries by pointer values.

        Deduplicates records which occur before or after a pointer on the current
        page - depending on whether log entries are in chronological or reverse
        chronological order. This enables deduplication of log events within the same
        page of results, and is intended to solve for cases where a provider's filters
        are not as granular as the pointer value.

        For example, some provider's only allow filtering on a date (YYYY-MM-DD) while
        returning log entries with timestamps that have millisecond precision.

        :param entries: A list of log entries to deduplicate.

        :return: A deduplicated list of log entries.
        """
        if self.LOG_ORDER == CHRONOLOGICAL:
            return self._deduplicate_by_pointer_chronological(entries)

        if self.LOG_ORDER == REVERSE_CHRONOLOGICAL:
            return self._deduplicate_by_pointer_reverse_chronological(entries)

    def _deduplicate_by_pointer_chronological(self, entries: List[Any]):
        """Deduplicates chronological log entries by their pointer.

        :param entries: A list of log entries to deduplicate.

        :return: A deduplicated list of log entries.
        """
        results = []
        pointer_passed = False

        for candidate in entries:
            candidate_pointer = str(jmespath.search(self.POINTER_PATH, candidate))

            if candidate_pointer == self.pointer:
                pointer_passed = True

            # Only track chronological records on and after the pointer.
            if pointer_passed:
                results.append(candidate)

        # If we never encountered the pointer, don't filter the records at all. This may
        # cause some duplicates if the pointer is on a subsequent page, but we always
        # prefer duplicates in these cases.
        if not pointer_passed:
            results = entries

        return results

    def _deduplicate_by_pointer_reverse_chronological(self, entries: List[Any]):
        """Deduplicates reverse chronological log entries by their pointer.

        :param entries: A list of log entries to deduplicate.

        :return: A deduplicated list of log entries.
        """
        results = []
        pointer_found = False
        pointer_passed = False

        for candidate in entries:
            candidate_pointer = jmespath.search(self.POINTER_PATH, candidate)

            if candidate_pointer == self.pointer:
                pointer_found = True

            if pointer_found and candidate_pointer != self.pointer:
                pointer_passed = True
                break

            if not pointer_passed:
                results.append(candidate)

        # If we never encountered the pointer, don't filter the records at all. This may
        # cause some duplicates if the pointer is on a subsequent page, but we always
        # prefer duplicates in these cases.
        if not pointer_passed:
            results = entries

        return results

    def process(self, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process log entries prior to saving.

        :param entries: A list of log entries to process.

        :return: A processed list of log entries.
        """
        # Shortcut where there are no processors configured.
        if len(self._processors) < 1:
            return []

        # As processors can modify the number of entries we need to do a few operations
        # here to handle additions and removals between iterations.
        processed = parsing.quick_copy(entries)

        for name, processor in self._processors.items():
            index = 0
            processed_size = len(processed)

            while index < processed_size:
                try:
                    processed[index:index] = processor.process(processed.pop(index))
                except Exception as err:
                    raise ProcessorError(
                        f"Fatal error in processor '{name}' ({processor}). {err}"
                    )

                # Adjust the loop index based on whether we've added or removed items.
                current_size = len(processed)
                if current_size >= processed_size:
                    index += 1

                processed_size = current_size

        return processed

    def finalize(self):
        """Performs final steps after each save operation has complete."""

        # Finalize all processors.
        for name, processor in self._processors.items():
            # Once again this exception handler is exceptionally (!) broad, to ensure
            # that any unhandled exception, including from downstream libraries, are
            # caught and handled consistently (except for BaseException derived).
            try:
                processor.finalize()
            except Exception as err:
                # As this runs after saving data and pointers, all we can really do is
                # log this and continue.
                self.logger.error(
                    f"Connector '{self.kind}' processor failed during finalization.",
                    extra={
                        "identity": name,
                        "processor": processor,
                        "exception": err,
                        **self.log_context,
                    },
                )

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
                    self._cache.get(
                        self.cache_key(CACHE_KEY_SEEN),
                        self.operation,
                    )
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
        self._lock_expiry = None
