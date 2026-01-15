import json
import glob
import os
import shutil

def find_and_clean_corrupted_files():
    """
    Find corrupted JSON files and move them to backup
    """
    print("=" * 70)
    print("SCANNING FOR CORRUPTED LOG FILES")
    print("=" * 70)
    
    run_files = glob.glob('logs/pipeline_runs/*.json')
    
    if not run_files:
        print("\nNo pipeline run files found!")
        return
    
    print(f"\nFound {len(run_files)} log file(s)")
    print("\nChecking each file...\n")
    
    valid_files = []
    corrupted_files = []
    
    for run_file in run_files:
        filename = os.path.basename(run_file)
        
        try:
            with open(run_file, 'r', encoding='utf-8') as f:
                json.load(f)
            
            print(f"[OK]   {filename}")
            valid_files.append(run_file)
            
        except json.JSONDecodeError as e:
            print(f"[CORRUPT] {filename} - JSONDecodeError: {e}")
            corrupted_files.append(run_file)
            
        except Exception as e:
            print(f"[ERROR] {filename} - {e}")
            corrupted_files.append(run_file)
    
    print("\n" + "=" * 70)
    print("SCAN RESULTS")
    print("=" * 70)
    print(f"Valid files:     {len(valid_files)}")
    print(f"Corrupted files: {len(corrupted_files)}")
    
    if corrupted_files:
        print("\nCorrupted files found:")
        for f in corrupted_files:
            print(f"   • {os.path.basename(f)}")
        
        print("\n" + "=" * 70)
        response = input("\nDo you want to move corrupted files to backup? (y/n): ").strip().lower()
        
        if response == 'y':
            # Create backup directory
            backup_dir = 'logs/pipeline_runs/corrupted'
            os.makedirs(backup_dir, exist_ok=True)
            
            print("\nMoving files...\n")
            
            for file_path in corrupted_files:
                filename = os.path.basename(file_path)
                backup_path = os.path.join(backup_dir, filename)
                
                try:
                    shutil.move(file_path, backup_path)
                    print(f"[OK] Moved: {filename}")
                except Exception as e:
                    print(f"[ERROR] Could not move {filename}: {e}")
            
            print("\nCleanup completed!")
            print(f"Corrupted files moved to: {backup_dir}")
        else:
            print("\n Corrupted files were NOT moved.")
            print("You can delete them manually or run this script again.")
    else:
        print("\nAll log files are valid!")
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    find_and_clean_corrupted_files()