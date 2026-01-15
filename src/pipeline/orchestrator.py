import logging
import time
from datetime import datetime
import traceback
import json
import os
import sys

# Import pipeline modules
from src.extract.weather_extractor import WeatherExtractor
from src.transform.weather_transformer import WeatherTransformer
from src.load.weather_loader import WeatherLoader
from src.analytics.weather_analytics import WeatherAnalytics
from src.pipeline.json_encoder import NumpyEncoder

# Fix Windows console encoding untuk emoji
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Setup logging dengan UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    """
    Orchestrate the complete ETL + Analytics pipeline
    """
    
    def __init__(self, cities=None):
        self.cities = cities or [
            'Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Semarang',
            'Makassar', 'Palembang', 'Denpasar', 'Singapore',
            'Kuala Lumpur', 'Bangkok', 'Manila', 'Ho Chi Minh City',
            'Hanoi', 'Yangon'
        ]
        
        self.extractor = WeatherExtractor()
        self.transformer = WeatherTransformer()
        self.loader = WeatherLoader()
        self.analytics = WeatherAnalytics()
        
        self.run_id = None
        self.run_stats = {}
        
        logger.info("Pipeline Orchestrator initialized")
    
    def generate_run_id(self):
        """Generate unique run ID"""
        self.run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        return self.run_id
    
    def convert_to_json_serializable(self, obj):
        """
        Convert numpy/pandas types to JSON-serializable types
        """
        if isinstance(obj, dict):
            return {k: self.convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_to_json_serializable(item) for item in obj]
        elif hasattr(obj, 'item'):  # numpy types
            return obj.item()
        elif hasattr(obj, 'isoformat'):  # datetime types
            return obj.isoformat()
        else:
            return obj
    
    def log_pipeline_start(self):
        """Log pipeline start"""
        self.run_stats = {
            'run_id': self.run_id,
            'start_time': datetime.now().isoformat(),
            'status': 'running',
            'stages': {}
        }
        
        logger.info("=" * 70)
        logger.info(f"[START] PIPELINE RUN: {self.run_id}")
        logger.info("=" * 70)
    
    def log_pipeline_end(self, status='success', error=None):
        """Log pipeline end"""
        self.run_stats['end_time'] = datetime.now().isoformat()
        self.run_stats['status'] = status
        self.run_stats['duration_seconds'] = (
            datetime.fromisoformat(self.run_stats['end_time']) - 
            datetime.fromisoformat(self.run_stats['start_time'])
        ).total_seconds()
        
        if error:
            self.run_stats['error'] = str(error)
        
        # Convert to JSON-serializable format
        json_safe_stats = self.convert_to_json_serializable(self.run_stats)
        
        # Save run stats
        os.makedirs('logs/pipeline_runs', exist_ok=True)
        stats_file = f"logs/pipeline_runs/{self.run_id}.json"
        
        try:
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(json_safe_stats, f, indent=2, cls=NumpyEncoder)
        except Exception as e:
            logger.error(f"Failed to save run stats: {e}")
        
        logger.info("=" * 70)
        if status == 'success':
            logger.info(f"[SUCCESS] PIPELINE RUN COMPLETED: {self.run_id}")
        else:
            logger.error(f"[FAILED] PIPELINE RUN: {self.run_id}")
            if error:
                logger.error(f"Error: {error}")
        logger.info(f"Duration: {self.run_stats['duration_seconds']:.2f} seconds")
        logger.info("=" * 70)
    
    def run_extraction(self):
        """Stage 1: Extract data from API"""
        stage_name = 'extraction'
        logger.info(f"\n[STAGE 1] EXTRACTION")
        logger.info("-" * 70)
        
        stage_start = time.time()
        
        try:
            # Extract data
            raw_data = self.extractor.extract_multiple_cities(self.cities, delay=1)
            
            if not raw_data:
                raise Exception("No data extracted")
            
            # Save raw data
            filename = f"weather_raw_{self.run_id}.json"
            filepath = self.extractor.save_raw_data(raw_data, filename)
            
            # Log stats
            self.run_stats['stages'][stage_name] = {
                'status': 'success',
                'records': len(raw_data),
                'filepath': filepath,
                'duration_seconds': time.time() - stage_start
            }
            
            logger.info(f"[OK] Extraction completed: {len(raw_data)} records")
            return raw_data, filepath
            
        except Exception as e:
            self.run_stats['stages'][stage_name] = {
                'status': 'failed',
                'error': str(e),
                'duration_seconds': time.time() - stage_start
            }
            logger.error(f"[ERROR] Extraction failed: {e}")
            raise
    
    def run_transformation(self, raw_filepath):
        """Stage 2: Transform data"""
        stage_name = 'transformation'
        logger.info(f"\n[STAGE 2] TRANSFORMATION")
        logger.info("-" * 70)
        
        stage_start = time.time()
        
        try:
            # Load raw data
            raw_data = self.transformer.load_raw_data(raw_filepath)
            
            # Flatten
            df_flat = self.transformer.flatten_weather_data(raw_data)
            logger.info(f"   Flattened: {df_flat.shape}")
            
            # Clean
            df_clean = self.transformer.clean_data(df_flat)
            logger.info(f"   Cleaned: {df_clean.shape}")
            
            # Enrich
            df_final = self.transformer.enrich_data(df_clean)
            logger.info(f"   Enriched: {df_final.shape}")
            
            # Save
            csv_path = self.transformer.save_transformed_data(
                df_final, 
                format='csv'
            )
            
            # Log stats
            self.run_stats['stages'][stage_name] = {
                'status': 'success',
                'records_input': int(len(df_flat)),
                'records_output': int(len(df_final)),
                'columns': int(len(df_final.columns)),
                'filepath': csv_path,
                'duration_seconds': time.time() - stage_start
            }
            
            logger.info(f"[OK] Transformation completed: {len(df_final)} records")
            return df_final, csv_path
            
        except Exception as e:
            self.run_stats['stages'][stage_name] = {
                'status': 'failed',
                'error': str(e),
                'duration_seconds': time.time() - stage_start
            }
            logger.error(f"[ERROR] Transformation failed: {e}")
            raise
    
    def run_loading(self, csv_filepath):
        """Stage 3: Load data to database"""
        stage_name = 'loading'
        logger.info(f"\n[STAGE 3] LOADING")
        logger.info("-" * 70)
        
        stage_start = time.time()
        
        try:
            # Load processed data
            df, source_file = self.loader.load_processed_data(csv_filepath)
            
            # Upsert city dimension
            self.loader.upsert_city_dimension(df)
            logger.info(f"   City dimension updated")
            
            # Load weather facts
            records_loaded = self.loader.load_weather_facts(df)
            logger.info(f"   Weather facts loaded: {records_loaded}")
            
            # Log history
            duration = time.time() - stage_start
            self.loader.log_load_history(
                records_count=records_loaded,
                source_file=source_file,
                status='success',
                duration=duration
            )
            
            # Log stats
            self.run_stats['stages'][stage_name] = {
                'status': 'success',
                'records_loaded': int(records_loaded),
                'duration_seconds': duration
            }
            
            logger.info(f"[OK] Loading completed: {records_loaded} records")
            
            # Get database stats
            db_stats = self.loader.get_load_statistics()
            logger.info(f"   Total records in DB: {db_stats['total_weather_records']}")
            
            return records_loaded
            
        except Exception as e:
            self.run_stats['stages'][stage_name] = {
                'status': 'failed',
                'error': str(e),
                'duration_seconds': time.time() - stage_start
            }
            logger.error(f"[ERROR] Loading failed: {e}")
            raise
        finally:
            self.loader.close()
    
    def run_analytics(self):
        """Stage 4: Generate analytics"""
        stage_name = 'analytics'
        logger.info(f"\n[STAGE 4] ANALYTICS")
        logger.info("-" * 70)
        
        stage_start = time.time()
        
        try:
            # Load data
            df = self.analytics.load_data()
            logger.info(f"   Loaded {len(df)} records for analysis")
            
            # Calculate KPIs
            kpis = self.analytics.calculate_kpis(df)
            logger.info(f"   KPIs calculated")
            
            # Generate static report
            self.analytics.create_static_summary_report(df, kpis)
            logger.info(f"   Static report generated")
            
            # Generate HTML dashboard
            dashboard_path = self.analytics.generate_html_dashboard(df, kpis)
            logger.info(f"   Dashboard generated: {dashboard_path}")
            
            # Convert KPIs to JSON-safe format
            json_safe_kpis = self.convert_to_json_serializable(kpis)
            
            # Log stats
            self.run_stats['stages'][stage_name] = {
                'status': 'success',
                'records_analyzed': int(len(df)),
                'dashboard_path': dashboard_path,
                'duration_seconds': time.time() - stage_start,
                'kpis': json_safe_kpis
            }
            
            logger.info(f"[OK] Analytics completed")
            return kpis
            
        except Exception as e:
            self.run_stats['stages'][stage_name] = {
                'status': 'failed',
                'error': str(e),
                'duration_seconds': time.time() - stage_start
            }
            logger.error(f"[ERROR] Analytics failed: {e}")
            raise
    
    def run_pipeline(self):
        """
        Execute complete pipeline: Extract -> Transform -> Load -> Analytics
        """
        self.generate_run_id()
        self.log_pipeline_start()
        
        try:
            # Stage 1: Extraction
            raw_data, raw_filepath = self.run_extraction()
            
            # Stage 2: Transformation
            df_transformed, csv_filepath = self.run_transformation(raw_filepath)
            
            # Stage 3: Loading
            records_loaded = self.run_loading(csv_filepath)
            
            # Stage 4: Analytics
            kpis = self.run_analytics()
            
            # Pipeline success
            self.log_pipeline_end(status='success')
            
            return {
                'status': 'success',
                'run_id': self.run_id,
                'stats': self.run_stats
            }
            
        except Exception as e:
            # Pipeline failed
            error_trace = traceback.format_exc()
            logger.error(f"Pipeline error traceback:\n{error_trace}")
            
            self.log_pipeline_end(status='failed', error=str(e))
            
            return {
                'status': 'failed',
                'run_id': self.run_id,
                'error': str(e),
                'stats': self.run_stats
            }
    
    def get_pipeline_history(self, limit=10):
        """Get recent pipeline run history"""
        import glob
        
        run_files = glob.glob('logs/pipeline_runs/*.json')
        run_files.sort(reverse=True)
        
        history = []
        for run_file in run_files[:limit]:
            try:
                with open(run_file, 'r', encoding='utf-8') as f:
                    history.append(json.load(f))
            except Exception as e:
                logger.warning(f"Could not load {run_file}: {e}")
                continue
        
        return history


def run_pipeline_once():
    """
    Run pipeline once
    """
    print("=" * 70)
    print("[PIPELINE] Weather Data Pipeline")
    print("=" * 70)
    
    orchestrator = PipelineOrchestrator()
    result = orchestrator.run_pipeline()
    
    print("\n" + "=" * 70)
    print("[SUMMARY] Pipeline Run Summary")
    print("=" * 70)
    print(f"Run ID: {result['run_id']}")
    print(f"Status: {result['status'].upper()}")
    print(f"Duration: {result['stats'].get('duration_seconds', 0):.2f} seconds")
    
    if result['status'] == 'success':
        print("\n[OK] All stages completed successfully!")
        print("\nGenerated files:")
        print(f"   - Dashboard: reports/dashboard.html")
        print(f"   - Report: reports/weather_summary.png")
        print(f"   - Logs: logs/pipeline_runs/{result['run_id']}.json")
    else:
        print(f"\n[ERROR] Pipeline failed: {result.get('error')}")
    
    print("=" * 70)
    
    return result