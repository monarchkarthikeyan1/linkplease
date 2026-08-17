import time
import httpx
import json
import os
from config import settings

MOCK_API = settings.PSEUDOGRAM_BASE_URL.rstrip('/')

def get_secret():
    if os.getenv("PSEUDOGRAM_API_KEY"):
        return os.getenv("PSEUDOGRAM_API_KEY").strip()
    if os.path.exists(".new_secret"):
        with open(".new_secret") as f:
            return f.read().strip()
    return settings.PSEUDOGRAM_API_KEY

API_KEY = get_secret()

def run_simulation(target_url: str, count: int = 500, duration_seconds: int = 10):
    base_url = target_url.rstrip('/')
    webhook_url = f"{base_url}/webhook"
    print(f"--- Starting 500-Event Production Simulation ---")
    print(f"Target Service Base URL: {base_url}")
    print(f"Webhook URL: {webhook_url}")
    print(f"Count: {count}, Duration: {duration_seconds}s")

    if not API_KEY:
        print("ERROR: PSEUDOGRAM_API_KEY environment variable is not set!")
        return

    with httpx.Client(timeout=30.0) as client:
        # 1. Create rule on target service with retry for container cold start
        print("\n1. Ensuring 'PRICE' rule exists on target service...")
        for attempt in range(5):
            try:
                rule_resp = client.post(
                    f"{base_url}/rules",
                    json={"keyword": "PRICE", "dm_message": "Here is the price list: https://example.com/prices"}
                )
                if rule_resp.status_code in (200, 201):
                    print(f"Rule setup status: {rule_resp.status_code}")
                    print(f"Rule response: {rule_resp.json()}")
                    break
                else:
                    print(f"Rule setup attempt {attempt+1} got status {rule_resp.status_code}, retrying in 3s...")
            except Exception as e:
                print(f"Rule setup warning: {e}")
            time.sleep(3)

        # 2. Trigger simulation on PseudoGram platform
        print("\n2. Triggering official 500-event simulation on PseudoGram platform...")
        sim_resp = client.post(
            f"{MOCK_API}/v1/simulate/start",
            headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            json={"webhook_url": webhook_url, "count": count, "duration_seconds": duration_seconds}
        )
        sim_data = sim_resp.json()
        print(f"Simulation response status: {sim_resp.status_code}")
        print(f"Simulation payload: {sim_data}")
        run_id = sim_data.get("run_id")
        if not run_id:
            print("ERROR: Failed to retrieve run_id from simulation response!")
            return

        # 3. Poll PseudoGram truth until generator completes
        print(f"\n3. Polling simulation truth for OFFICIAL run_id={run_id}...")
        truth_data = {}
        while True:
            try:
                truth_resp = client.get(f"{MOCK_API}/v1/simulate/{run_id}/truth", headers={"X-API-Key": API_KEY})
                truth_data = truth_resp.json()
                status = truth_data.get("status")
                print(f"Simulation generator status: {status}")
                if status == "complete":
                    print(f"\n=== PseudoGram Server-Side Truth for Run {run_id} ===")
                    print(json.dumps(truth_data, indent=2))
                    break
            except Exception as e:
                print(f"Poll warning: {e}")
            time.sleep(3)

        # 4. Poll target service /stats until background queue drains
        print(f"\n4. Polling target {base_url}/stats until queue drains (queued == 0)...")
        while True:
            try:
                stats_resp = client.get(f"{base_url}/stats")
                if stats_resp.status_code == 200:
                    stats = stats_resp.json()
                    print(f"Current /stats: {stats}")
                    if isinstance(stats, dict) and stats.get("queued", 1) == 0:
                        print("\nAll queued DMs processed and reconciled!")
                        break
                else:
                    print(f"Stats check HTTP {stats_resp.status_code}, retrying...")
            except Exception as e:
                print(f"Stats warning: {e}")
            time.sleep(5)

        final_stats = client.get(f"{base_url}/stats").json()
        print(f"\n=== Final Production /stats ===")
        print(json.dumps(final_stats, indent=2))

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python test_simulation.py <production_service_url> [count] [duration_seconds]")
        sys.exit(1)
    
    target = sys.argv[1]
    cnt = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    dur = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    run_simulation(target, count=cnt, duration_seconds=dur)
