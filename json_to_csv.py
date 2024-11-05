import json
import csv
import sys

def write_dict_to_csv(data, csv_filename, category):
        with open(csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write the header
            writer.writerow([category, 'beginner_games', 'intermediate_games', 'advanced_games'])
            # Write the data
            for key, value in data.items():
                beginner_games = value.get('beginner', {}).get('games', 0)
                intermediate_games = value.get('intermediate', {}).get('games', 0)
                advanced_games = value.get('advanced', {}).get('games', 0)
                writer.writerow([key, beginner_games, intermediate_games, advanced_games])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 json_to_csv.py <json_file>")
        sys.exit(1)
    
    with open(sys.argv[1]) as json_file:
        data = json.load(json_file)

    # Convert each category to a CSV file
    write_dict_to_csv(data['openings'], 'openings.csv', 'opening')
    write_dict_to_csv(data['time_controls'], 'time_controls.csv', 'time_control')
    write_dict_to_csv(data['ecos'], 'ecos.csv', 'eco')
    write_dict_to_csv(data['terminations'], 'terminations.csv', 'termination')

    print("CSV files have been created successfully.")
