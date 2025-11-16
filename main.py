from csv_parser import CSVParser

def print_section(title):
    print("\n" + "="*60)
    print(title)
    print("="*60 + "\n")

def test_csv_parser():
    file_path = "data/database_24_25.csv"

    print_section("1. Initializing CSVParser")
    try:
        parser = CSVParser(file_path)
        print("CSVParser initialized successfully.")
    except Exception as e:
        print(f"Initialization failed: {e}")
        return

    print_section("2. Testing parse()")
    try:
        data = parser.parse()
        print(f"Parsed {len(data)} rows.")
    except Exception as e:
        print(f"parse() failed: {e}")
        return

    print_section("3. Testing get_data()")
    try:
        data = parser.get_data()
        print(f"Data length: {len(data)}")
        print("First row:")
        print(data[0])
    except Exception as e:
        print(f"get_data() failed: {e}")

    print_section("4. Testing get_schema()")
    try:
        schema = parser.get_schema()
        print("Schema:")
        for col, dtype in schema.items():
            print(f"{col}: {dtype}")
    except Exception as e:
        print(f"get_schema() failed: {e}")

    print_section("5. Testing filter_columns()")
    try:
        # Use actual column names from your CSV schema
        example_cols = ["Player", "PTS"]
        filtered = parser.filter_columns(example_cols)
        print(f"Filtered first 3 rows ({example_cols}):")
        for row in filtered[:3]:
            print(row)
    except Exception as e:
        print(f"filter_columns() failed: {e}")

    print_section("6. Testing summary()")
    try:
        summary = parser.summary()
        print("Summary:")
        for key, value in summary.items():
            print(f"{key}: {value}")
    except Exception as e:
        print(f"summary() failed: {e}")

    print_section("7. FULL TEST COMPLETE")
    print("All CSVParser functions executed.")


if __name__ == "__main__":
    test_csv_parser()
