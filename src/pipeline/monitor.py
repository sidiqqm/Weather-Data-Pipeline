import json
import glob
from datetime import datetime
from tabulate import tabulate
import pandas as pd
import os

def get_pipeline_history(limit=20):
    """Get pipeline run history dengan error handling"""
    run_files = glob.glob('logs/pipeline_runs/*.json')
    run_files.sort(reverse=True)
    
    history = []
    corrupted_files = []
    
    for run_file in run_files[:limit]:
        try:
            with open(run_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                history.append(data)
        except json.JSONDecodeError as e:
            # JSON file corrupt
            corrupted_files.append(run_file)
            print(f"[WARNING] Skipping corrupted file: {os.path.basename(run_file)}")
            continue
        except Exception as e:
            # Other errors
            corrupted_files.append(run_file)
            print(f"[WARNING] Could not load {os.path.basename(run_file)}: {e}")
            continue
    
    return history, corrupted_files

def clean_corrupted_files(corrupted_files):
    """Move corrupted files to backup folder"""
    if not corrupted_files:
        return
    
    backup_dir = 'logs/pipeline_runs/corrupted'
    os.makedirs(backup_dir, exist_ok=True)
    
    for file_path in corrupted_files:
        try:
            filename = os.path.basename(file_path)
            backup_path = os.path.join(backup_dir, filename)
            
            # Move to backup
            os.rename(file_path, backup_path)
            print(f"[INFO] Moved corrupted file to: {backup_path}")
        except Exception as e:
            print(f"[ERROR] Could not move {file_path}: {e}")

def display_pipeline_status():
    """Display pipeline status dashboard"""
    print("=" * 100)
    print("PIPELINE MONITORING DASHBOARD")
    print("=" * 100)
    
    history, corrupted_files = get_pipeline_history(limit=20)
    
    if corrupted_files:
        print(f"\n  Found {len(corrupted_files)} corrupted file(s)")
        response = input("Do you want to move them to backup? (y/n): ").strip().lower()
        if response == 'y':
            clean_corrupted_files(corrupted_files)
            print()
    
    if not history:
        print("\n  No valid pipeline runs found!")
        print("\nPossible reasons:")
        print("  1. No pipeline has been run yet")
        print("  2. All run files are corrupted")
        print("\nTry running: python run_pipeline.py")
        return
    
    # Overall statistics
    total_runs = len(history)
    successful_runs = sum(1 for run in history if run.get('status') == 'success')
    failed_runs = total_runs - successful_runs
    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
    
    print(f"\n OVERALL STATISTICS")
    print("-" * 100)
    print(f"Total Runs: {total_runs}")
    print(f"Successful: {successful_runs} ({success_rate:.1f}%)")
    print(f"Failed: {failed_runs} ({100-success_rate:.1f}%)")
    
    if corrupted_files:
        print(f"Corrupted Files: {len(corrupted_files)} (excluded from stats)")
    
    # Latest run details
    latest = history[0]
    print(f"\n LATEST RUN DETAILS")
    print("-" * 100)
    print(f"Run ID: {latest.get('run_id', 'N/A')}")
    
    status = latest.get('status', 'unknown')
    status_display = '[OK] SUCCESS' if status == 'success' else '[ERROR] FAILED'
    print(f"Status: {status_display}")
    
    print(f"Start Time: {latest.get('start_time', 'N/A')}")
    print(f"Duration: {latest.get('duration_seconds', 0):.2f} seconds")
    
    if status == 'success':
        stages = latest.get('stages', {})
        if stages:
            print(f"\nStage Details:")
            for stage_name, stage_data in stages.items():
                duration = stage_data.get('duration_seconds', 0)
                records = stage_data.get('records', 
                         stage_data.get('records_loaded', 
                         stage_data.get('records_output', 'N/A')))
                print(f"   • {stage_name.capitalize()}: {duration:.2f}s, Records: {records}")
    else:
        error = latest.get('error', 'Unknown error')
        print(f"\n Error: {error}")
    
    # Recent runs table
    print(f"\nRECENT RUNS (Last 10)")
    print("-" * 100)
    
    table_data = []
    for run in history[:10]:
        status = run.get('status', 'unknown')
        status_emoji = "[OK]" if status == 'success' else "[FAIL]"
        duration = run.get('duration_seconds', 0)
        
        # Count successful stages
        stages = run.get('stages', {})
        successful_stages = sum(1 for s in stages.values() if s.get('status') == 'success')
        total_stages = len(stages)
        
        start_time = run.get('start_time', 'N/A')
        if start_time != 'N/A':
            start_time = start_time[:19]  # Trim to datetime only
        
        table_data.append([
            run.get('run_id', 'N/A'),
            f"{status_emoji} {status}",
            start_time,
            f"{duration:.2f}s",
            f"{successful_stages}/{total_stages}" if total_stages > 0 else "0/0"
        ])
    
    print(tabulate(
        table_data,
        headers=['Run ID', 'Status', 'Start Time', 'Duration', 'Stages'],
        tablefmt='grid'
    ))
    
    # Performance trends
    print(f"\nPERFORMANCE TRENDS")
    print("-" * 100)
    
    durations = [run.get('duration_seconds', 0) for run in history if run.get('duration_seconds')]
    if durations:
        print(f"Average Duration: {sum(durations)/len(durations):.2f}s")
        print(f"Min Duration: {min(durations):.2f}s")
        print(f"Max Duration: {max(durations):.2f}s")
    else:
        print("No duration data available")
    
    print("\n" + "=" * 100)

def display_pipeline_errors():
    """Display failed pipeline runs"""
    print("=" * 100)
    print("FAILED PIPELINE RUNS")
    print("=" * 100)
    
    history, corrupted_files = get_pipeline_history(limit=50)
    failed_runs = [run for run in history if run.get('status') == 'failed']
    
    if not failed_runs:
        print("\nNo failed runs found!")
        if corrupted_files:
            print(f"\n  However, {len(corrupted_files)} corrupted file(s) were skipped")
        return
    
    print(f"\nTotal Failed Runs: {len(failed_runs)}")
    print("-" * 100)
    
    for i, run in enumerate(failed_runs[:10], 1):
        print(f"\n{i}. Run ID: {run.get('run_id', 'N/A')}")
        print(f"   Time: {run.get('start_time', 'N/A')}")
        print(f"   Error: {run.get('error', 'Unknown error')}")
        
        # Show which stage failed
        stages = run.get('stages', {})
        for stage_name, stage_data in stages.items():
            if stage_data.get('status') == 'failed':
                print(f"   Failed Stage: {stage_name}")
                print(f"   Stage Error: {stage_data.get('error', 'Unknown')}")
    
    if len(failed_runs) > 10:
        print(f"\n... and {len(failed_runs) - 10} more failed runs")
    
    print("\n" + "=" * 100)

def display_pipeline_summary():
    """Display quick summary"""
    history, corrupted_files = get_pipeline_history(limit=100)
    
    if not history:
        print("No pipeline runs found")
        return
    
    total = len(history)
    successful = sum(1 for r in history if r.get('status') == 'success')
    failed = total - successful
    
    print("=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"Total Runs:     {total}")
    print(f"Successful:     {successful} ({successful/total*100:.1f}%)")
    print(f"Failed:         {failed} ({failed/total*100:.1f}%)")
    
    if corrupted_files:
        print(f"Corrupted:      {len(corrupted_files)}")
    
    if history:
        latest = history[0]
        print(f"\nLatest Run:     {latest.get('run_id', 'N/A')}")
        print(f"Status:         {latest.get('status', 'N/A').upper()}")
        print(f"Duration:       {latest.get('duration_seconds', 0):.2f}s")
    
    print("=" * 60)

if __name__ == "__main__":
    display_pipeline_status()
    print("\n")
    display_pipeline_errors()