import csv
import time
import sys

def main():
    if len(sys.argv) != 3:
        print("Usage: python py-etl.py input.csv output.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    print("Starting Python ETL with standard library...")
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")

    start_time = time.time()

    try:
        processed_count = 0
        eligible_count = 0
        invalid_count = 0

        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
             open(output_file, 'w', newline='', encoding='utf-8') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            header = next(reader)
            writer.writerow(header)

            try:
                electric_range_index = header.index('Electric Range')
            except ValueError:
                # Essayer avec le nom exact du fichier
                electric_range_index = header.index('Electric Range') if 'Electric Range' in header else -1

            if electric_range_index == -1:
                print("Error: Column 'Electric Range' not found")
                sys.exit(1)

            for row in reader:
                processed_count += 1

                try:
                    electric_range_str = row[electric_range_index]
                    if electric_range_str.strip() == '':
                        invalid_count += 1
                        continue

                    electric_range = int(electric_range_str)

                    if electric_range > 200:
                        writer.writerow(row)
                        eligible_count += 1

                except (ValueError, IndexError):
                    invalid_count += 1
                    continue

        end_time = time.time()
        duration = end_time - start_time

        print(f"Total processed: {processed_count} records")
        print(f"Eligible: {eligible_count} records")
        print(f"Invalid/Skipped: {invalid_count} records")
        print(f"Python stdlib ETL completed in {duration:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()