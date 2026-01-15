import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherAnalytics:
    """
    Analytics and visualization for weather data
    """
    
    def __init__(self, connection_string=None):
        if connection_string is None:
            connection_string = (
                "postgresql://weather_user:weather_pass123@localhost:5432/weather_analytics"
            )
        
        self.engine = create_engine(connection_string)
        logger.info("WeatherAnalytics initialized")
    
    def load_data(self):
        """
        Load all weather data from database
        """
        logger.info("Loading data from database...")
        
        query = """
        SELECT 
            city_name,
            country_code,
            temp_celsius,
            feels_like_celsius,
            temp_category,
            humidity_percent,
            humidity_category,
            pressure_hpa,
            wind_speed_mps,
            wind_strength,
            cloudiness_percent,
            visibility_meters,
            weather_main,
            weather_description,
            is_raining,
            is_snowing,
            weather_severity,
            daylight_hours,
            heat_index_diff,
            dt_datetime,
            extracted_at_datetime,
            date,
            hour,
            day_of_week,
            month,
            longitude,
            latitude
        FROM weather_fact
        ORDER BY dt_datetime DESC;
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f" Loaded {len(df)} records")
        
        return df
    
    def calculate_kpis(self, df):
        """
        Calculate Key Performance Indicators
        """
        kpis = {
            'total_cities': df['city_name'].nunique(),
            'total_countries': df['country_code'].nunique(),
            'avg_temperature': df['temp_celsius'].mean(),
            'max_temperature': df['temp_celsius'].max(),
            'min_temperature': df['temp_celsius'].min(),
            'avg_humidity': df['humidity_percent'].mean(),
            'avg_wind_speed': df['wind_speed_mps'].mean(),
            'cities_with_rain': df['is_raining'].sum(),
            'hottest_city': df.loc[df['temp_celsius'].idxmax(), 'city_name'],
            'coldest_city': df.loc[df['temp_celsius'].idxmin(), 'city_name'],
            'most_humid_city': df.loc[df['humidity_percent'].idxmax(), 'city_name'],
            'windiest_city': df.loc[df['wind_speed_mps'].idxmax(), 'city_name'],
        }
        
        return kpis
    
    def create_temperature_map(self, df):
        """
        Create interactive map showing temperature across cities
        """
        logger.info("Creating temperature map...")
        
        fig = px.scatter_geo(
            df,
            lat='latitude',
            lon='longitude',
            hover_name='city_name',
            hover_data={
                'temp_celsius': ':.1f',
                'humidity_percent': ':.0f',
                'weather_main': True,
                'latitude': False,
                'longitude': False
            },
            size='temp_celsius',
            color='temp_celsius',
            color_continuous_scale='RdYlBu_r',
            title='Temperature Map Across Cities',
            labels={'temp_celsius': 'Temperature (°C)', 'humidity_percent': 'Humidity (%)'}
        )
        
        fig.update_layout(
            geo=dict(
                scope='asia',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastlinecolor='rgb(204, 204, 204)',
            ),
            height=600,
            font=dict(size=12)
        )
        
        return fig
    
    def create_temperature_comparison(self, df):
        """
        Bar chart comparing temperatures across cities
        """
        logger.info("Creating temperature comparison...")
        
        # Sort by temperature
        df_sorted = df.sort_values('temp_celsius', ascending=False)
        
        fig = go.Figure()
        
        # Actual temperature
        fig.add_trace(go.Bar(
            name='Actual Temp',
            x=df_sorted['city_name'],
            y=df_sorted['temp_celsius'],
            marker_color='#FF6B6B',
            text=df_sorted['temp_celsius'].round(1),
            textposition='outside'
        ))
        
        # Feels like temperature
        fig.add_trace(go.Bar(
            name='Feels Like',
            x=df_sorted['city_name'],
            y=df_sorted['feels_like_celsius'],
            marker_color='#4ECDC4',
            text=df_sorted['feels_like_celsius'].round(1),
            textposition='outside'
        ))
        
        fig.update_layout(
            title='🌡️ Temperature Comparison Across Cities',
            xaxis_title='City',
            yaxis_title='Temperature (°C)',
            barmode='group',
            height=500,
            hovermode='x unified',
            font=dict(size=11),
            xaxis={'tickangle': -45}
        )
        
        return fig
    
    def create_weather_condition_distribution(self, df):
        """
        Pie chart of weather conditions
        """
        logger.info("Creating weather condition distribution...")
        
        weather_counts = df['weather_main'].value_counts()
        
        fig = px.pie(
            values=weather_counts.values,
            names=weather_counts.index,
            title='Weather Condition Distribution',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
        )
        
        fig.update_layout(height=450, font=dict(size=12))
        
        return fig
    
    def create_humidity_wind_scatter(self, df):
        """
        Scatter plot: Humidity vs Wind Speed
        """
        logger.info("Creating humidity-wind scatter plot...")
        
        fig = px.scatter(
            df,
            x='humidity_percent',
            y='wind_speed_mps',
            size='temp_celsius',
            color='weather_main',
            hover_name='city_name',
            hover_data={'temp_celsius': ':.1f', 'cloudiness_percent': ':.0f'},
            title='Humidity vs Wind Speed',
            labels={
                'humidity_percent': 'Humidity (%)',
                'wind_speed_mps': 'Wind Speed (m/s)',
                'temp_celsius': 'Temperature (°C)'
            },
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        
        fig.update_layout(height=500, font=dict(size=11))
        
        return fig
    
    def create_country_statistics(self, df):
        """
        Bar chart: Average metrics by country
        """
        logger.info("Creating country statistics...")
        
        country_stats = df.groupby('country_code').agg({
            'temp_celsius': 'mean',
            'humidity_percent': 'mean',
            'wind_speed_mps': 'mean',
            'city_name': 'count'
        }).round(2).reset_index()
        
        country_stats.columns = ['Country', 'Avg Temp (°C)', 'Avg Humidity (%)', 'Avg Wind (m/s)', 'Cities']
        country_stats = country_stats.sort_values('Avg Temp (°C)', ascending=False)
        
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=('Average Temperature', 'Average Humidity', 'Average Wind Speed'),
            specs=[[{'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}]]
        )
        
        # Temperature
        fig.add_trace(
            go.Bar(
                x=country_stats['Country'],
                y=country_stats['Avg Temp (°C)'],
                marker_color='#FF6B6B',
                name='Temperature',
                text=country_stats['Avg Temp (°C)'],
                textposition='outside'
            ),
            row=1, col=1
        )
        
        # Humidity
        fig.add_trace(
            go.Bar(
                x=country_stats['Country'],
                y=country_stats['Avg Humidity (%)'],
                marker_color='#4ECDC4',
                name='Humidity',
                text=country_stats['Avg Humidity (%)'],
                textposition='outside'
            ),
            row=1, col=2
        )
        
        # Wind Speed
        fig.add_trace(
            go.Bar(
                x=country_stats['Country'],
                y=country_stats['Avg Wind (m/s)'],
                marker_color='#95E1D3',
                name='Wind',
                text=country_stats['Avg Wind (m/s)'],
                textposition='outside'
            ),
            row=1, col=3
        )
        
        fig.update_layout(
            title_text='Weather Statistics by Country',
            showlegend=False,
            height=500,
            font=dict(size=10)
        )
        
        fig.update_xaxes(tickangle=-45)
        
        return fig
    
    def create_temperature_categories(self, df):
        """
        Stacked bar chart: Temperature categories by country
        """
        logger.info("Creating temperature categories chart...")
        
        temp_cat_country = pd.crosstab(
            df['country_code'],
            df['temp_category'],
            normalize='index'
        ) * 100
        
        fig = go.Figure()
        
        colors = {'Cold': '#4A90E2', 'Cool': '#7ED321', 'Warm': '#F5A623', 'Hot': '#D0021B'}
        
        for category in ['Cold', 'Cool', 'Warm', 'Hot']:
            if category in temp_cat_country.columns:
                fig.add_trace(go.Bar(
                    name=category,
                    x=temp_cat_country.index,
                    y=temp_cat_country[category],
                    marker_color=colors[category],
                    text=temp_cat_country[category].round(1),
                    texttemplate='%{text}%',
                    textposition='inside'
                ))
        
        fig.update_layout(
            title='Temperature Categories by Country (%)',
            xaxis_title='Country',
            yaxis_title='Percentage (%)',
            barmode='stack',
            height=450,
            font=dict(size=11)
        )
        
        return fig
    
    def create_static_summary_report(self, df, kpis, output_path='reports/weather_summary.png'):
        """
        Create static summary report using Matplotlib
        """
        logger.info("Creating static summary report...")
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Weather Data Summary Report', fontsize=20, fontweight='bold', y=0.995)
        
        # 1. Temperature distribution
        ax1 = axes[0, 0]
        df['temp_celsius'].hist(bins=20, ax=ax1, color='#FF6B6B', edgecolor='black', alpha=0.7)
        ax1.axvline(df['temp_celsius'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {df["temp_celsius"].mean():.1f}°C')
        ax1.set_title('Temperature Distribution', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Temperature (°C)', fontsize=12)
        ax1.set_ylabel('Frequency', fontsize=12)
        ax1.legend()
        ax1.grid(alpha=0.3)
        
        # 2. Top 10 cities by temperature
        ax2 = axes[0, 1]
        top_cities = df.nlargest(10, 'temp_celsius')[['city_name', 'temp_celsius']]
        ax2.barh(top_cities['city_name'], top_cities['temp_celsius'], color='#4ECDC4')
        ax2.set_title('Top 10 Hottest Cities', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Temperature (°C)', fontsize=12)
        ax2.invert_yaxis()
        ax2.grid(axis='x', alpha=0.3)
        
        # 3. Humidity vs Temperature scatter
        ax3 = axes[1, 0]
        scatter = ax3.scatter(df['temp_celsius'], df['humidity_percent'], 
                             c=df['wind_speed_mps'], cmap='viridis', 
                             s=100, alpha=0.6, edgecolors='black')
        ax3.set_title('Temperature vs Humidity (colored by Wind Speed)', fontsize=14, fontweight='bold')
        ax3.set_xlabel('Temperature (°C)', fontsize=12)
        ax3.set_ylabel('Humidity (%)', fontsize=12)
        ax3.grid(alpha=0.3)
        plt.colorbar(scatter, ax=ax3, label='Wind Speed (m/s)')
        
        # 4. Key metrics table
        ax4 = axes[1, 1]
        ax4.axis('off')
        
        metrics_data = [
            ['Metric', 'Value'],
            ['Total Cities', f"{kpis['total_cities']}"],
            ['Total Countries', f"{kpis['total_countries']}"],
            ['Avg Temperature', f"{kpis['avg_temperature']:.1f}°C"],
            ['Max Temperature', f"{kpis['max_temperature']:.1f}°C ({kpis['hottest_city']})"],
            ['Min Temperature', f"{kpis['min_temperature']:.1f}°C ({kpis['coldest_city']})"],
            ['Avg Humidity', f"{kpis['avg_humidity']:.1f}%"],
            ['Avg Wind Speed', f"{kpis['avg_wind_speed']:.1f} m/s"],
            ['Cities with Rain', f"{kpis['cities_with_rain']}"],
            ['Most Humid City', f"{kpis['most_humid_city']}"],
            ['Windiest City', f"{kpis['windiest_city']}"],
        ]
        
        table = ax4.table(cellText=metrics_data, cellLoc='left', loc='center',
                         colWidths=[0.5, 0.5])
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1, 2.5)
        
        # Style header row
        for i in range(2):
            table[(0, i)].set_facecolor('#4ECDC4')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        # Alternate row colors
        for i in range(1, len(metrics_data)):
            for j in range(2):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor('#F0F0F0')
        
        ax4.set_title('Key Metrics Summary', fontsize=14, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
        # Save
        os.makedirs('reports', exist_ok=True)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Static report saved to {output_path}")
        
        plt.close()
    
    def generate_html_dashboard(self, df, kpis, output_path='reports/dashboard.html'):
        """
        Generate complete HTML dashboard
        """
        logger.info("Generating HTML dashboard...")
        
        # Create all visualizations
        fig_map = self.create_temperature_map(df)
        fig_temp_comparison = self.create_temperature_comparison(df)
        fig_weather_dist = self.create_weather_condition_distribution(df)
        fig_humidity_wind = self.create_humidity_wind_scatter(df)
        fig_country_stats = self.create_country_statistics(df)
        fig_temp_categories = self.create_temperature_categories(df)
        
        # Generate HTML
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weather Analytics Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 3em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }}
        
        .header p {{
            font-size: 1.2em;
            opacity: 0.9;
        }}
        
        .kpi-container {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f8f9fa;
        }}
        
        .kpi-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            text-align: center;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}
        
        .kpi-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.2);
        }}
        
        .kpi-card .icon {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .kpi-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }}
        
        .kpi-card .label {{
            color: #6c757d;
            font-size: 1em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .chart-container {{
            padding: 40px;
        }}
        
        .chart {{
            background: white;
            border-radius: 15px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        
        .footer {{
            background: #2d3436;
            color: white;
            text-align: center;
            padding: 20px;
            font-size: 0.9em;
        }}
        
        @media (max-width: 768px) {{
            .header h1 {{
                font-size: 2em;
            }}
            
            .kpi-container {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <h1>Weather Analytics Dashboard</h1>
            <p>Real-time Weather Data Analysis | Generated on {datetime.now().strftime('%B %d, %Y at %H:%M')}</p>
        </div>
        
        <!-- KPI Cards -->
        <div class="kpi-container">
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['total_cities']}</div>
                <div class="label">Total Cities</div>
            </div>
            
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['avg_temperature']:.1f}°C</div>
                <div class="label">Avg Temperature</div>
            </div>
            
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['max_temperature']:.1f}°C</div>
                <div class="label">Max Temperature</div>
            </div>
            
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['min_temperature']:.1f}°C</div>
                <div class="label">Min Temperature</div>
            </div>
            
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['avg_humidity']:.0f}%</div>
                <div class="label">Avg Humidity</div>
            </div>
            
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['avg_wind_speed']:.1f} m/s</div>
                <div class="label">Avg Wind Speed</div>
            </div>
            
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['cities_with_rain']}</div>
                <div class="label">Cities with Rain</div>
            </div>
            
            <div class="kpi-card">
                <div class="icon"></div>
                <div class="value">{kpis['hottest_city']}</div>
                <div class="label">Hottest City</div>
            </div>
        </div>
        
        <!-- Charts -->
        <div class="chart-container">
            <div class="chart" id="map"></div>
            <div class="chart" id="temp-comparison"></div>
            <div class="chart" id="weather-dist"></div>
            <div class="chart" id="humidity-wind"></div>
            <div class="chart" id="country-stats"></div>
            <div class="chart" id="temp-categories"></div>
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <p>Weather Data Pipeline | Built with Python, PostgreSQL, and Plotly | © 2025</p>
        </div>
    </div>
    
    <script>
        // Render charts
        var mapData = {fig_map.to_json()};
        Plotly.newPlot('map', mapData.data, mapData.layout, {{responsive: true}});
        
        var tempCompData = {fig_temp_comparison.to_json()};
        Plotly.newPlot('temp-comparison', tempCompData.data, tempCompData.layout, {{responsive: true}});
        
        var weatherDistData = {fig_weather_dist.to_json()};
        Plotly.newPlot('weather-dist', weatherDistData.data, weatherDistData.layout, {{responsive: true}});
        
        var humidityWindData = {fig_humidity_wind.to_json()};
        Plotly.newPlot('humidity-wind', humidityWindData.data, humidityWindData.layout, {{responsive: true}});
        
        var countryStatsData = {fig_country_stats.to_json()};
        Plotly.newPlot('country-stats', countryStatsData.data, countryStatsData.layout, {{responsive: true}});
        
        var tempCatData = {fig_temp_categories.to_json()};
        Plotly.newPlot('temp-categories', tempCatData.data, tempCatData.layout, {{responsive: true}});
    </script>
</body>
</html>
"""
        
        # Save HTML
        os.makedirs('reports', exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML dashboard saved to {output_path}")
        
        return output_path


def generate_complete_analytics():
    """
    Main function to generate all analytics
    """
    print("=" * 70)
    print("WEATHER DATA ANALYTICS & VISUALIZATION")
    print("=" * 70)
    
    analytics = WeatherAnalytics()
    
    # Load data
    print("\n Loading data from database...")
    df = analytics.load_data()
    print(f" Loaded {len(df)} records")
    
    # Calculate KPIs
    print("\nCalculating KPIs...")
    kpis = analytics.calculate_kpis(df)
    print(" KPIs calculated")
    
    print("\nKey Metrics:")
    print(f"   • Total Cities: {kpis['total_cities']}")
    print(f"   • Avg Temperature: {kpis['avg_temperature']:.1f}°C")
    print(f"   • Hottest City: {kpis['hottest_city']} ({kpis['max_temperature']:.1f}°C)")
    print(f"   • Coldest City: {kpis['coldest_city']} ({kpis['min_temperature']:.1f}°C)")
    
    # Generate static report
    print("\n Generating static summary report...")
    analytics.create_static_summary_report(df, kpis)
    print(" Static report generated: reports/weather_summary.png")
    
    # Generate HTML dashboard
    print("\n Generating interactive HTML dashboard...")
    dashboard_path = analytics.generate_html_dashboard(df, kpis)
    print(f" Dashboard generated: {dashboard_path}")
    
    print("\n" + "=" * 70)
    print(" ANALYTICS GENERATION COMPLETED!")
    print("=" * 70)
    print("\n Output files:")
    print("   • reports/weather_summary.png (Static Report)")
    print("   • reports/dashboard.html (Interactive Dashboard)")
    print("\n Open dashboard.html in your browser to view interactive charts!")
    
    return df, kpis