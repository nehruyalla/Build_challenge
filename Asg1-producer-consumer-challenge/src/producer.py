import threading
import time
import random
from typing import Any
from .blocking_queue import BoundedBlockingQueue

class Producer(threading.Thread):
    """
    Producer thread that generates items and puts them into the queue.
    """

    def __init__(self, thread_id: int, queue: BoundedBlockingQueue, items_to_produce: int, delay_range: tuple[float, float] = (0.01, 0.1)) -> None:
        """
        Initialize the Producer.

        Args:
            thread_id (int): Identifier for the thread.
            queue (BoundedBlockingQueue): The shared queue.
            items_to_produce (int): Number of items to generate.
            delay_range (tuple): Min and max delay in seconds between items.
        """
        super().__init__(name=f"Producer-{thread_id}")
        self.thread_id = thread_id
        self.queue = queue
        self.items_to_produce = items_to_produce
        self.delay_range = delay_range

    def run(self) -> None:
        """
        Main execution loop of the producer thread.

        Generates a specified number of items and puts them into the queue.
        Simulates work by sleeping for a random duration before producing each item.
        """
        print(f"[{self.name}] Started.")
        for i in range(1, self.items_to_produce + 1):
            item = f"Item-{self.thread_id}-{i}"
            
            # Simulate work
            time.sleep(random.uniform(*self.delay_range))
            
            print(f"[{self.name}] Producing {item}...")
            self.queue.put(item)
            print(f"[{self.name}] Put {item} into queue (Size: {self.queue.qsize()})")
        
        print(f"[{self.name}] Finished producing {self.items_to_produce} items.")
