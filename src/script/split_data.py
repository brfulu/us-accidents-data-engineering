import os
import csv


def split_csv(source_filepath, dest_folder, split_file_prefix, records_per_file):
    """
    Split a source csv into multiple csvs of equal numbers of records,
    except the last file.

    Includes the initial header row in each split file.

    Split files follow a zero-index sequential naming convention like so:

        `{split_file_prefix}_0.csv`
    """
    if records_per_file <= 0:
        raise Exception('records_per_file must be > 0')

    with open(source_filepath, 'r') as source:
        reader = csv.reader(source)
        headers = next(reader)

        file_idx = 0
        records_exist = True

        while records_exist:
            i = 0
            target_filename = f'{split_file_prefix}_{file_idx}.csv'
            target_filepath = os.path.join(dest_folder, target_filename)
            os.makedirs(os.path.dirname(target_filepath), exist_ok=True)

            with open(target_filepath, 'w') as target:
                writer = csv.writer(target)

                while i < records_per_file:
                    if i == 0:
                        writer.writerow(headers)

                    try:
                        writer.writerow(next(reader))
                        i += 1
                    except:
                        records_exist = False
                        break

            if i == 0:
                # we only wrote the header, so delete that file
                os.remove(target_filepath)

            file_idx += 1


def main():
    split_csv('../dataset/us_accidents.csv', '../dataset/accident_data/', 'us_accidents_part', 40000)
    split_csv('../dataset/airport_codes.csv', '../dataset/airport_data/', 'airport_codes_part', 10000)
    split_csv('../dataset/us_cities_demographics.csv', '../dataset/city_data/', 'us_cities_demographics_part', 1000)


if __name__ == '__main__':
    main()
