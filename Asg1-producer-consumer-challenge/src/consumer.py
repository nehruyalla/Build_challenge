import threading
import time
import random
from typing import Any
from .blocking_queue import BoundedBlockingQueue

class Consumer(threading.Thread):
    """
    Consumer thread that retrieves items from the queue and processes them.
    Stops when it receives a sentinel value (None).
    """

    def __init__(self, thread_id: int, queue: BoundedBlockingQueue, delay_range: tuple[float, float] = (0.05, 0.2)) -> None:
        """
        Initialize the Consumer.

        Args:
            thread_id (int): Identifier for the thread.
            queue (BoundedBlockingQueue): The shared queue.
            delay_range (tuple): Min and max delay in seconds for processing.
        """
        super().__init__(name=f"Consumer-{thread_id}")
        self.thread_id = thread_id
        self.queue = queue
        self.delay_range = delay_range
        self.consumed_count = 0

    def run(self) -> None:
        """
        Main execution loop of the consumer thread.

        Continuously retrieves items from the queue and processes them.
        The loop terminates when a sentinel value (None) is retrieved.
        """
        print(f"[{self.name}] Started.")
        while True:
            item = self.queue.get()
            
            if item is None:
                # Sentinel value received, stop consuming
                print(f"[{self.name}] Received stop signal. Exiting.")
                # Put the sentinel back for other consumers if needed? 
                # Ideally, main sends one sentinel per consumer.
                # Here we assume 1 sentinel per consumer.
                break
            
            print(f"[{self.name}] Retrieved {item} (Size: {self.queue.qsize()})")
            
            # Simulate processing
            time.sleep(random.uniform(*self.delay_range))
            self.consumed_count += 1
            print(f"[{self.name}] Processed {item}")
        
        print(f"[{self.name}] Finished. Total consumed: {self.consumed_count}")
