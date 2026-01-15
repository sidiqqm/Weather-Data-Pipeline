import json
import glob
import os

# Find latest pipeline run
run_files = glob.glob('logs/pipeline_runs/*.json')
if not run_files:
    print("No pipeline run files found!")
    exit()

latest_file = max(run_files, key=os.path.getctime)

print("=" * 70)
print("Verifying JSON Output")
print("=" * 70)
print(f"\nFile: {latest_file}\n")

try:
    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print("[OK] JSON file is valid!")
    print(f"\nRun ID: {data['run_id']}")
    print(f"Status: {data['status']}")
    print(f"Duration: {data.get('duration_seconds', 0):.2f}s")
    
    print("\nStages:")
    for stage_name, stage_data in data.get('stages', {}).items():
        status = stage_data.get('status', 'unknown')
        duration = stage_data.get('duration_seconds', 0)
        print(f"  - {stage_name}: {status} ({duration:.2f}s)")
    
    print("\n[OK] All data types are JSON-serializable!")
    
except json.JSONDecodeError as e:
    print(f"[ERROR] Invalid JSON: {e}")
except Exception as e:
    print(f"[ERROR] {e}")