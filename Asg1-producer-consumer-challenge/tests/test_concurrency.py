import pytest
import threading
import time
from src.blocking_queue import BoundedBlockingQueue
from src.producer import Producer
from src.consumer import Consumer

def test_producer_consumer_integration() -> None:
    """
    Integration test for Producer and Consumer classes with BoundedBlockingQueue.

    Simulates a real-world scenario with multiple producers and consumers.
    Verifies that all produced items are consumed and the final queue state is clean.
    """
    # Setup
    queue = BoundedBlockingQueue(10)
    producers = [Producer(i, queue, 5, (0, 0.01)) for i in range(2)] # 2 producers, 5 items each = 10 items total
    consumers = [Consumer(i, queue, (0, 0.01)) for i in range(2)] # 2 consumers
    
    # Start
    for c in consumers:
        c.start()
    for p in producers:
        p.start()
        
    # Wait for producers
    for p in producers:
        p.join()
        
    # Send Stop Signals
    for _ in consumers:
        queue.put(None)
        
    # Wait for consumers
    for c in consumers:
        c.join()
        
    # Verify
    # Total items produced: 10
    # Consumed count check
    total_consumed = sum(c.consumed_count for c in consumers)
    assert total_consumed == 10
    assert queue.qsize() == 0 # Queue should be empty (sentinels also consumed)

def test_consumer_wait_logic() -> None:
    """
    Test that a consumer correctly waits (blocks) when the queue is empty.

    Starts a consumer on an empty queue, waits to ensure it blocks, then provides data
    and verifies that the consumer resumes and processes the item.
    """
    # Ensure consumers wait if queue is empty
    queue = BoundedBlockingQueue(5)
    consumer = Consumer(1, queue, (0, 0))
    consumer.start()
    
    # Give it a moment to block on get()
    time.sleep(0.1)
    assert consumer.is_alive()
    
    # Provide data
    queue.put("test")
    
    # Stop
    queue.put(None)
    consumer.join()
    
    assert consumer.consumed_count == 1
