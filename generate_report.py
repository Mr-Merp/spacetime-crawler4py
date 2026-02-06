from analytics import get_analytics, generate_report

if __name__ == "__main__":
    print("Generating crawler analytics report...")
    print()

    # Get the analytics instance (loads from analytics_data.json if exists)
    analytics = get_analytics()

    # Generate and print the report
    generate_report()

    print()
    print(f"Report saved to REPORT.txt")
    print(f"Analytics data saved to analytics_data.json")
