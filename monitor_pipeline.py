from src.pipeline.monitor import display_pipeline_status, display_pipeline_errors
import sys

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'errors':
        display_pipeline_errors()
    else:
        display_pipeline_status()
        
        print("\nTip: Run 'python monitor_pipeline.py errors' to see only failed runs")