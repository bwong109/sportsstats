from csv_parser import CSVParser

def main():
    csv_file = "data/database_24_25.csv"  # replace with your actual CSV path
    parser = CSVParser(csv_file)

    # -----------------
    # Parse CSV
    # -----------------
    parser.parse()  # full parse into memory
    print("CSV Parsed!")
    print("Schema:", parser.get_schema())
    print("Sample row:", parser.summary()["sample_row"])
    print("-" * 50)

    # -----------------
    # Column access (__getitem__)
    # -----------------
    print("First 5 PTS values:", parser["PTS"][:5])
    print("-" * 50)

    # -----------------
    # Filter rows
    # -----------------
    high_scorers = parser.filter_rows(lambda r: r["PTS"] >= 30)
    print(f"Number of players with >=30 points: {len(high_scorers)}")
    print("Top 5 high scorers:", high_scorers[:5])
    print("-" * 50)

    # -----------------
    # Column projection
    # -----------------
    projected = parser.filter_columns(["Player", "Tm", "PTS"])
    print("Projected sample (Player, Tm, PTS):", projected[:5])
    print("-" * 50)

    # -----------------
    # Aggregation: Average points per team
    # -----------------
    avg_pts_per_team = parser.aggregate(group_by="Tm", target_col="PTS", func="avg")
    print("Average points per team:", avg_pts_per_team)
    print("-" * 50)

    # -----------------
    # Aggregation: Total rebounds per team
    # -----------------
    total_reb_per_team = parser.aggregate(group_by="Tm", target_col="TRB", func="sum")
    print("Total rebounds per team:", total_reb_per_team)
    print("-" * 50)

    # -----------------
    # Join example
    # -----------------
    # Mock another dataset with team coaches
    coaches_data = [
        {"Tm": "LAL", "Coach": "Darvin Ham"},
        {"Tm": "GSW", "Coach": "Steve Kerr"},
        {"Tm": "BKN", "Coach": "Jacque Vaughn"},
    ]
    joined = parser.join(coaches_data, left_on="Tm", right_on="Tm")
    print("Joined sample (Player + Coach):", joined[:5])
    print("-" * 50)

    # -----------------
    # Chunked parsing example (scaling)
    # -----------------
    print("Demonstrating chunked parsing (5 rows per chunk):")
    chunk_parser = CSVParser(csv_file)
    for i, chunk in enumerate(chunk_parser.parse(chunk_size=5)):
        print(f"Chunk {i+1}:", chunk)
        if i >= 2:  # just show first 3 chunks
            break

if __name__ == "__main__":
    main()
