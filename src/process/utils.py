from threading import Condition
from queue import Queue

def process_task_on_queue(task, finalizingTask, queue: Queue, condition: Condition):
    while True:
        with condition:
            while queue.empty():
                condition.wait()
            data = queue.get()
            condition.notify()

        if data is None:
            print(f"Received end of {task} signal")
            finalizingTask()
            break

        task(data)

def push_to_queue_syc(data, queue: Queue, condition: Condition):
    with condition:
        while queue.full():
            condition.wait()
        queue.put(data)
        condition.notify()

def noop():
    pass
