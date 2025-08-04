from flask import Flask
from threading import Thread, Lock
import time

app = Flask(__name__)
worker_states = ["idle"] * 4
lock = Lock()

def background_job_scheduler():
    job_id = 1
    while True:
        with lock:
            if "idle" in worker_states:
                worker_index = worker_states.index("idle")
                worker_states[worker_index] = "busy"
                print(f"Assigned Job {job_id} to Worker {worker_index}")
                Thread(target=process_job, args=(worker_index, job_id)).start()
                job_id += 1
        time.sleep(1)  # Adjust delay to your need

def process_job(worker_index, job_id):
    print(f"Worker {worker_index} started Job {job_id}")
    time.sleep(5)  # Simulate job time
    with lock:
        worker_states[worker_index] = "idle"
    print(f"Worker {worker_index} completed Job {job_id}")

@app.route("/start")
def start_jobs():
    Thread(target=background_job_scheduler, daemon=True).start()
    return "Started continuous job assignment!\n"

if __name__ == "__main__":
    app.run()
