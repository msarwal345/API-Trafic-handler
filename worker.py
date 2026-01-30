from time import sleep
import time,redis,requests
import json 
redis_client = redis.Redis(host='127.0.0.1', port=6789, db=0)
print("Worker started. Listening on api_limiter queue...")
def process_queue():
    while True:
        all_keys=redis_client.keys("api_limiter_*")    
        if not all_keys:
            sleep(1)
            continue
        queue_name=all_keys[0]
        task=redis_client.lpop(queue_name)
        if task:
            task_data=json.loads(task)
            try:
                response=requests.request(
                    method=task_data['method'],
                    url=task_data['url'],
                    data=task_data['body'],
                    headers=task_data['headers'],
                    timeout=5
                )
                print("Task processed. Status code:",response.status_code)
            except Exception as e:
                print("Error processing task:",e)

if __name__ == "__main__":
    process_queue()