import pytest
import threading
import time
from src.blocking_queue import BoundedBlockingQueue

def test_queue_initialization() -> None:
    """
    Test the initialization of the BoundedBlockingQueue.

    Verifies that the queue is created with the correct capacity and initial size.
    Also ensures that invalid capacities (<= 0) raise a ValueError.
    """
    q = BoundedBlockingQueue(5)
    assert q.qsize() == 0
    assert q.capacity == 5
    
    with pytest.raises(ValueError):
        BoundedBlockingQueue(0)
        
    with pytest.raises(ValueError):
        BoundedBlockingQueue(-1)

def test_put_get_basic() -> None:
    """
    Test basic put and get operations without blocking.

    Verifies that items can be added and removed from the queue and that the size is updated correctly.
    """
    q = BoundedBlockingQueue(5)
    q.put(1)
    q.put(2)
    assert q.qsize() == 2
    
    assert q.get() == 1
    assert q.get() == 2
    assert q.qsize() == 0

def test_blocking_get() -> None:
    """
    Test that get() blocks when the queue is empty.

    Starts a separate thread to put an item into the queue after a delay.
    Verifies that get() waits for the item to be available.
    """
    q = BoundedBlockingQueue(5)
    start_time = time.time()
    
    def delayed_put() -> None:
        """Helper function to put an item into the queue after a delay."""
        time.sleep(0.5)
        q.put("item")
        
    t = threading.Thread(target=delayed_put)
    t.start()
    
    item = q.get()
    duration = time.time() - start_time
    
    assert item == "item"
    assert duration >= 0.5  # Should have blocked
    t.join()

def test_blocking_put() -> None:
    """
    Test that put() blocks when the queue is full.

    Fills the queue to capacity and starts a separate thread to remove an item after a delay.
    Verifies that put() waits for space to become available.
    """
    q = BoundedBlockingQueue(1)
    q.put("first")
    
    start_time = time.time()
    
    def delayed_get() -> None:
        """Helper function to get an item from the queue after a delay."""
        time.sleep(0.5)
        q.get()
        
    t = threading.Thread(target=delayed_get)
    t.start()
    
    q.put("second") # Should block until delayed_get runs
    duration = time.time() - start_time
    
    assert duration >= 0.5
    assert q.qsize() == 1 # "first" removed, "second" added
    t.join()

def test_fifo_order() -> None:
    """
    Test that the queue maintains FIFO (First-In-First-Out) order.

    Puts a sequence of items into the queue and verifies that they are retrieved in the same order.
    """
    q = BoundedBlockingQueue(10)
    inputs = list(range(10))
    
    for i in inputs:
        q.put(i)
        
    outputs = []
    for _ in inputs:
        outputs.append(q.get())
        
    assert inputs == outputs
