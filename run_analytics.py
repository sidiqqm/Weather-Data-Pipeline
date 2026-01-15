from src.analytics.weather_analytics import generate_complete_analytics
import webbrowser
import os

if __name__ == "__main__":
    # Generate analytics
    df, kpis = generate_complete_analytics()
    
    # Ask user if they want to open dashboard
    print("\n" + "=" * 70)
    response = input("open the dashboard in your browser? (y/n): ")
    
    if response.lower() == 'y':
        dashboard_path = os.path.abspath('reports/dashboard.html')
        webbrowser.open(f'file://{dashboard_path}')
        print("Dashboard opened in browser!")
    else:
        print("💡 You can open 'reports/dashboard.html' manually later.")