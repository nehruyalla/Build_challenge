import time
from src.blocking_queue import BoundedBlockingQueue
from src.producer import Producer
from src.consumer import Consumer

def main() -> None:
    """
    Main entry point for the Producer-Consumer demonstration.

    Sets up the shared blocking queue, initializes producer and consumer threads,
    starts them, waits for completion, and handles graceful shutdown using sentinel values.
    """
    print("=== Producer-Consumer Challenge Demo ===")
    
    # Configuration
    QUEUE_CAPACITY = 5
    NUM_PRODUCERS = 2
    NUM_CONSUMERS = 3
    ITEMS_PER_PRODUCER = 10
    
    # Initialize shared queue
    queue = BoundedBlockingQueue(QUEUE_CAPACITY)
    
    # Create Producers
    producers = []
    for i in range(NUM_PRODUCERS):
        p = Producer(thread_id=i+1, queue=queue, items_to_produce=ITEMS_PER_PRODUCER)
        producers.append(p)
    
    # Create Consumers
    consumers = []
    for i in range(NUM_CONSUMERS):
        c = Consumer(thread_id=i+1, queue=queue)
        consumers.append(c)
    
    # Start Consumers
    for c in consumers:
        c.start()
        
    # Start Producers
    for p in producers:
        p.start()
        
    # Wait for producers to finish
    for p in producers:
        p.join()
    print("--- All Producers Finished ---")
    
    # Signal consumers to stop (send one sentinel per consumer)
    print("--- Sending Shutdown Signals ---")
    for _ in range(NUM_CONSUMERS):
        queue.put(None)
        
    # Wait for consumers to finish
    for c in consumers:
        c.join()
        
    print("=== Demo Completed Successfully ===")

if __name__ == "__main__":
    main()
