import threading
from typing import Generic, TypeVar, List, Optional
import time

T = TypeVar('T')

class BoundedBlockingQueue(Generic[T]):
    """
    A thread-safe bounded blocking queue implemented using threading.Condition.
    
    Attributes:
        capacity (int): The maximum number of items the queue can hold.
        queue (List[T]): Internal storage for queue items.
        lock (threading.Lock): Lock for thread safety.
        not_full (threading.Condition): Condition variable for "not full" state.
        not_empty (threading.Condition): Condition variable for "not empty" state.
    """

    def __init__(self, capacity: int) -> None:
        """
        Initialize the queue with a specific capacity.

        Args:
            capacity (int): Maximum number of items. Must be > 0.
        """
        if capacity <= 0:
            raise ValueError("Capacity must be greater than 0")
        
        self.capacity = capacity
        self.queue: List[T] = []
        self.lock = threading.Lock()
        # Condition variables share the same lock
        self.not_full = threading.Condition(self.lock)
        self.not_empty = threading.Condition(self.lock)

    def put(self, item: T) -> None:
        """
        Add an item to the queue. 
        Blocks if the queue is full until space becomes available.

        Args:
            item (T): The item to add.
        """
        with self.not_full:
            while len(self.queue) >= self.capacity:
                # Wait until there is space
                self.not_full.wait()
            
            self.queue.append(item)
            # Notify consumers that an item is available
            self.not_empty.notify()

    def get(self) -> T:
        """
        Remove and return an item from the queue.
        Blocks if the queue is empty until an item is available.

        Returns:
            T: The item removed from the queue.
        """
        with self.not_empty:
            while len(self.queue) == 0:
                # Wait until there is an item
                self.not_empty.wait()
            
            item = self.queue.pop(0)
            # Notify producers that there is space
            self.not_full.notify()
            return item

    def qsize(self) -> int:
        """
        Return the approximate size of the queue.
        
        Returns:
            int: Number of items in the queue.
        """
        with self.lock:
            return len(self.queue)
