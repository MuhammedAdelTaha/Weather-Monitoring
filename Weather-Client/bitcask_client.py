#!/usr/bin/env python3
import requests
import sys
import csv
import time
from concurrent.futures import ThreadPoolExecutor

# Connect to Docker container's exposed port
BASE_URL = "http://localhost:8080"  # Change if using different port mapping

def view_all():
    try:
        response = requests.get(f"{BASE_URL}/stations", timeout=10)
        response.raise_for_status()
        
        timestamp = int(time.time())
        filename = f"{timestamp}.csv"
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["key", "value"])
            for key, value in response.json().items():
                writer.writerow([key, value])
                
        print(f"Data saved to {filename}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to server: {str(e)}", file=sys.stderr)
        sys.exit(1)

def view_key(key):
    try:
        response = requests.get(f"{BASE_URL}/station", 
                              params={"id": key},
                              timeout=5)
        if response.status_code == 404:
            print(f"Key '{key}' not found")
            sys.exit(1)
        response.raise_for_status()
        print(response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

def perf_test(num_clients):
    def worker(thread_num):
        try:
            response = requests.get(f"{BASE_URL}/stations", timeout=10)
            data = response.json()
            
            timestamp = int(time.time())
            filename = f"{timestamp}_thread_{thread_num}.csv"
            
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["key", "value"])
                for key, value in data.items():
                    writer.writerow([key, value])
                    
            return thread_num, True
        except requests.exceptions.RequestException as _:
            return thread_num, False

    print(f"Starting performance test with {num_clients} clients...")
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        results = list(executor.map(worker, range(1, num_clients + 1)))
    
    success = sum(1 for _, success in results if success)
    duration = time.time() - start_time
    
    print(f"\nPerformance test completed in {duration:.2f} seconds")
    print(f"Successful requests: {success}/{num_clients}")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  ./bitcask_client.py --view-all")
        print("  ./bitcask_client.py --view --key=SOME_KEY")
        print("  ./bitcask_client.py --perf --clients=NUM_CLIENTS")
        sys.exit(1)

    command = sys.argv[1]

    if command == "--view-all":
        view_all()
    elif command == "--view":
        if len(sys.argv) < 3 or not sys.argv[2].startswith("--key="):
            print("Error: Missing key parameter", file=sys.stderr)
            sys.exit(1)
        key = sys.argv[2].split("=")[1]
        view_key(key)
    elif command == "--perf":
        if len(sys.argv) < 3 or not sys.argv[2].startswith("--clients="):
            print("Error: Missing clients parameter", file=sys.stderr)
            sys.exit(1)
        try:
            num_clients = int(sys.argv[2].split("=")[1])
            perf_test(num_clients)
        except ValueError:
            print("Error: Clients must be a number", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()