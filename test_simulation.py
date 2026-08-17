import time
import httpx
import json
from config import settings

MOCK_API = settings.PSEUDOGRAM_BASE_URL.rstrip('/')
API_KEY = settings.PSEUDOGRAM_API_KEY

def run_simulation(tunnel_url: str, count: int = 500, duration_seconds: int = 10):
    webhook_url = f"{tunnel_url.rstrip('/')}/webhook"
    print(f"--- Starting Simulation ---")
    print(f"Webhook URL: {webhook_url}")
    print(f"Count: {count}, Duration: {duration_seconds}s")

    with httpx.Client(timeout=30.0) as client:
        # 1. Create test rule
        rule_resp = client.post(
            "http://localhost:8000/rules",
            json={"keyword": "PRICE", "dm_message": "Here is the price list!"}
        )
        print(f"Rule setup: {rule_resp.status_code} -> {rule_resp.json()}")

        # 2. Trigger simulation
        sim_resp = client.post(
            f"{MOCK_API}/v1/simulate/start",
            headers={"X-API-Key": API_KEY, "Content-Type": "application/json"},
            json={"webhook_url": webhook_url, "count": count, "duration_seconds": duration_seconds}
        )
        sim_data = sim_resp.json()
        print(f"Simulation response: {sim_resp.status_code} -> {sim_data}")
        run_id = sim_data.get("run_id")
        if not run_id:
            print("Failed to start simulation!")
            return

        # 3. Poll truth until complete
        print(f"Polling simulation truth for run_id={run_id}...")
        while True:
            try:
                truth_resp = client.get(f"{MOCK_API}/v1/simulate/{run_id}/truth", headers={"X-API-Key": API_KEY})
                truth_data = truth_resp.json()
                status = truth_data.get("status")
                print(f"Simulation status: {status}")
                if status == "complete":
                    print("\n=== Simulation Complete Truth ===")
                    print(json.dumps(truth_data, indent=2))
                    break
            except Exception as e:
                print(f"Poll warning: {e}")
            time.sleep(3)

        # 4. Wait for local background workers to complete processing queue
        print("\nWaiting for local workers to process outbound queue & reconcile status...")
        while True:
            stats_resp = client.get("http://localhost:8000/stats")
            stats = stats_resp.json()
            print(f"Current /stats: {stats}")
            if stats["queued"] == 0:
                print("All queued DMs processed!")
                break
            time.sleep(5)

        final_stats = client.get("http://localhost:8000/stats").json()
        print("\n=== Final Local /stats ===")
        print(json.dumps(final_stats, indent=2))

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python test_simulation.py <public_tunnel_url> [count] [duration_seconds]")
        sys.exit(1)
    
    tunnel = sys.argv[1]
    cnt = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    dur = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    run_simulation(tunnel, count=cnt, duration_seconds=dur)
