# Producer-Consumer Challenge (Python)

This project implements the classic **Producer-Consumer pattern** using Python's `threading` module. It features a custom **Bounded Blocking Queue** implemented with `threading.Condition` to demonstrate low-level thread synchronization mechanisms (Wait/Notify).

## Features

- **Custom Blocking Queue**: Implements `put()` and `get()` with `wait()` and `notify()` logic, ensuring thread safety and blocking behavior when the queue is full or empty.
- **Producer Threads**: Generate data and wait if the queue is full.
- **Consumer Threads**: Process data and wait if the queue is empty.
- **Graceful Shutdown**: Uses sentinel values (`None`) to signal consumers to stop processing.
- **Unit & Integration Tests**: Comprehensive testing using `pytest`.

## Project Structure

```
producer-consumer-challenge/
├── src/
│   ├── blocking_queue.py  # Custom BoundedBlockingQueue implementation
│   ├── producer.py        # Producer thread class
│   ├── consumer.py        # Consumer thread class
│   └── main.py            # Entry point
├── tests/
│   ├── test_blocking_queue.py
│   └── test_concurrency.py
├── scripts/
│   ├── run.sh             # Helper to run the application
│   └── test.sh            # Helper to run tests
├── requirements.txt       # Dependencies
└── README.md              # Documentation
```

## Setup

1. **Prerequisites**: Python 3.x, `uv` (optional, for dependency management).
2. **Install Dependencies**:

   ```bash
   # Using uv
   uv venv
   source .venv/bin/activate
   uv pip install -r requirements.txt

   # Or using standard pip
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

## Running the Application

Use the helper script:

```bash
./scripts/run.sh
```

Or run directly:

```bash
python -m src.main
```

## Running Tests

Use the helper script:

```bash
./scripts/test.sh
```

Or run directly:

```bash
pytest tests
```

## Implementation Details

### BoundedBlockingQueue

The core of this project is the `BoundedBlockingQueue` class in `src/blocking_queue.py`. It uses:

- `threading.Lock`: To ensure mutual exclusion.
- `threading.Condition`: Two condition variables (`not_full`, `not_empty`) to manage blocking states.
  - Producers wait on `not_full` if the queue is at capacity.
  - Consumers wait on `not_empty` if the queue is empty.
