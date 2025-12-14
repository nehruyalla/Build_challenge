"""Functional utilities for stream processing.

This module provides functional programming utilities for working with
iterators and streams, enabling composition and transformation.
"""

import itertools
from collections.abc import Callable, Iterator
from typing import TypeVar

T = TypeVar("T")
U = TypeVar("U")


def broadcast(stream: Iterator[T], n: int) -> tuple[Iterator[T], ...]:
    """Broadcast (tee) a stream into N independent iterators.

    This allows multiple consumers to process the same stream. Each iterator
    can be consumed independently.

    Warning: The tee objects maintain an internal buffer. If one iterator
    is consumed much faster than others, memory usage will grow.

    Args:
        stream: Source iterator to broadcast
        n: Number of independent iterators to create

    Returns:
        Tuple of N independent iterators

    Examples:
        >>> nums = iter([1, 2, 3])
        >>> s1, s2 = broadcast(nums, 2)
        >>> list(s1)
        [1, 2, 3]
        >>> list(s2)
        [1, 2, 3]
    """
    return itertools.tee(stream, n)


def partition(
    stream: Iterator[T], predicate: Callable[[T], bool]
) -> tuple[Iterator[T], Iterator[T]]:
    """Partition a stream into two iterators based on a predicate.

    Returns:
        (true_iterator, false_iterator) - Elements where predicate is True/False

    Note: This consumes the entire stream and stores results in memory.
    Use only when the split is necessary and datasets are manageable.

    Args:
        stream: Source iterator to partition
        predicate: Function to test each element

    Returns:
        Tuple of (true_items, false_items)

    Examples:
        >>> nums = iter([1, 2, 3, 4, 5])
        >>> evens, odds = partition(nums, lambda x: x % 2 == 0)
        >>> list(evens)
        [2, 4]
        >>> list(odds)
        [1, 3, 5]
    """
    # We need to consume the stream once and split it
    true_items = []
    false_items = []

    for item in stream:
        if predicate(item):
            true_items.append(item)
        else:
            false_items.append(item)

    return iter(true_items), iter(false_items)


def fold(stream: Iterator[T], reducer: Callable[[U, T], U], initial: U) -> U:
    """Fold (reduce) a stream using a reducer function.

    This is a functional programming pattern for aggregation.

    Args:
        stream: Iterator to fold
        reducer: Function that combines accumulator and current value
        initial: Initial accumulator value

    Returns:
        Final accumulated value

    Examples:
        >>> nums = iter([1, 2, 3, 4])
        >>> fold(nums, lambda acc, x: acc + x, 0)
        10
        >>> words = iter(['hello', 'world'])
        >>> fold(words, lambda acc, w: acc + [len(w)], [])
        [5, 5]
    """
    accumulator = initial
    for item in stream:
        accumulator = reducer(accumulator, item)
    return accumulator


def take(stream: Iterator[T], n: int) -> Iterator[T]:
    """Take the first N items from a stream.

    Args:
        stream: Source iterator
        n: Number of items to take

    Yields:
        Up to N items from the stream

    Examples:
        >>> nums = iter(range(100))
        >>> list(take(nums, 5))
        [0, 1, 2, 3, 4]
    """
    return itertools.islice(stream, n)


def drop(stream: Iterator[T], n: int) -> Iterator[T]:
    """Drop the first N items from a stream and yield the rest.

    Args:
        stream: Source iterator
        n: Number of items to drop

    Yields:
        Items after the first N

    Examples:
        >>> nums = iter(range(10))
        >>> list(drop(nums, 5))
        [5, 6, 7, 8, 9]
    """
    return itertools.islice(stream, n, None)


def filter_stream(stream: Iterator[T], predicate: Callable[[T], bool]) -> Iterator[T]:
    """Filter a stream based on a predicate.

    Args:
        stream: Source iterator
        predicate: Function to test each element

    Yields:
        Items where predicate returns True

    Examples:
        >>> nums = iter([1, 2, 3, 4, 5])
        >>> list(filter_stream(nums, lambda x: x % 2 == 0))
        [2, 4]
    """
    return filter(predicate, stream)


def map_stream(stream: Iterator[T], func: Callable[[T], U]) -> Iterator[U]:
    """Map a function over a stream.

    Args:
        stream: Source iterator
        func: Function to apply to each element

    Yields:
        Transformed items

    Examples:
        >>> nums = iter([1, 2, 3])
        >>> list(map_stream(nums, lambda x: x * 2))
        [2, 4, 6]
    """
    return map(func, stream)


def chunk_stream(stream: Iterator[T], size: int) -> Iterator[list[T]]:
    """Chunk a stream into fixed-size batches.

    Args:
        stream: Source iterator
        size: Size of each chunk

    Yields:
        Lists of up to `size` items

    Examples:
        >>> nums = iter(range(10))
        >>> list(chunk_stream(nums, 3))
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    iterator = iter(stream)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk

