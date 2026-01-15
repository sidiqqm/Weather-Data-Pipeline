from src.pipeline.scheduler import start_scheduler
import sys

if __name__ == "__main__":
    print("=" * 70)
    print("WEATHER DATA PIPELINE SCHEDULER")
    print("=" * 70)
    print("\nSchedule Modes:")
    print("1. Interval - Run every N minutes (default: 30 min)")
    print("2. Hourly - Run every hour")
    print("3. Daily - Run once per day at specific time")
    print("=" * 70)
    
    mode_input = input("\nSelect mode (1/2/3) [default: 1]: ").strip()
    
    if mode_input == '2':
        print("\nStarting hourly scheduler...")
        start_scheduler(mode='hourly')
    
    elif mode_input == '3':
        time_input = input("Enter run time (HH:MM) [default: 08:00]: ").strip()
        run_time = time_input if time_input else "08:00"
        print(f"\nStarting daily scheduler (run time: {run_time})...")
        start_scheduler(mode='daily', daily_time=run_time)
    
    else:  # Default to interval
        interval_input = input("Enter interval in minutes [default: 30]: ").strip()
        interval = int(interval_input) if interval_input.isdigit() else 30
        print(f"\n Starting interval scheduler (every {interval} minutes)...")
        start_scheduler(mode='interval', interval_minutes=interval)