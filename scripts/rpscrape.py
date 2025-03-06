#!/usr/bin/env python3

import gzip
import requests
import os
import sys
import csv
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from lxml import html

from dataclasses import dataclass

from lxml import html
from orjson import loads

from utils.argparser import ArgParser
from utils.completer import Completer
from utils.header import RandomHeader
from utils.race import Race, VoidRaceError
from utils.settings import Settings
from utils.update import Update

from utils.course import course_name, courses
from utils.lxml_funcs import xpath

settings = Settings()
random_header = RandomHeader()


@dataclass
class RaceList:
    course_id: str
    course_name: str
    url: str


def check_for_update():
    update = Update()

    if update.available():
        choice = input('Update available. Do you want to update? Y/N ')
        if choice.lower() != 'y':
            return

        if update.pull_latest():
            print('Updated successfully.')
        else:
            print('Failed to update.')

        sys.exit()


def get_race_urls(tracks, years, code):
    urls = set()

    url_course = 'https://www.racingpost.com:443/profile/course/filter/results'
    url_result = 'https://www.racingpost.com/results'

    race_lists = []

    for track in tracks:
        for year in years:
            url = f'{url_course}/{track[0]}/{year}/{code}/all-races'
            race_list = RaceList(*track, url)
            race_lists.append(race_list)

    for race_list in race_lists:
        r = requests.get(race_list.url, headers=random_header.header())
        races = loads(r.text)['data']['principleRaceResults']

        if races:
            for race in races:
                race_date = race['raceDatetime'][:10]
                race_id = race['raceInstanceUid']
                url = f'{url_result}/{race_list.course_id}/{race_list.course_name}/{race_date}/{race_id}'
                urls.add(url.replace(' ', '-').replace("'", ''))

    return sorted(list(urls))


def get_race_urls_date(dates, region):
    urls = set()

    days = [f'https://www.racingpost.com/results/{d}' for d in dates]

    course_ids = {course[0] for course in courses(region)}

    for day in days:
        r = requests.get(day, headers=random_header.header())
        doc = html.fromstring(r.content)

        races = xpath(doc, 'a', 'link-listCourseNameLink')

        for race in races:
            if race.attrib['href'].split('/')[2] in course_ids:
                urls.add('https://www.racingpost.com' + race.attrib['href'])

    return sorted(list(urls))

def amend_csv(file_path):
    """
    Amend the generated CSV file by adding a new column 'amended ts'
    with a formula N2 + (154 - K2) for each row.
    """
    print(f"Starting to amend the CSV file: {file_path}")
    
    temp_file = file_path + '.tmp'  # Temporary file to store amended data
    print(f"Creating a temporary file: {temp_file}")

    try:
        with open(file_path, 'r', encoding='utf-8') as infile, open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
            print(f"Opened {file_path} for reading and {temp_file} for writing.")
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Read the header row and add the new column
            header = next(reader)
            print(f"Original header: {header}")
            header.append('amended ts')  # Add the new column
            writer.writerow(header)
            print("Amended the header and wrote it to the temporary file.")
            
            # Process and amend the rows
            for row in reader:
                # Extract N2 and K2 from the row, assuming N2 is in column 0 and K2 in column 1
                # Convert to numeric values for calculation
                try:
                    n2 = float(row[13])  # Adjust column index if N2 is not in the 0th column
                    k2 = float(row[10])  # Adjust column index if K2 is not in the 1st column
                    amended_value = n2 + (154 - k2)  # Compute the amended value
                except ValueError:
                    # Handle rows where conversion fails (e.g., if N2/K2 are not numbers)
                    amended_value = 0
                
                row.append(amended_value)  # Add the calculated value to the new column
                
                writer.writerow(row)  # Write the updated row to the temporary file
            print("Processed all rows, added amendments")
    except Exception as e:
        print(f"Error occurred while amending the CSV: {e}")
        raise

    # Replace original file with amended file
    os.replace(temp_file, file_path)
    print(f"Replaced original CSV with amended CSV: {file_path}")
    
def amend_csv_remove_columns(file_path):
    """
    Remove columns with specific headers ('lbs', 'rpr', 'ts') from the CSV file.
    """
    headers_to_remove = ["lbs", "rpr", "ts"]  # Headers to target for removal
    print(f"Starting to process the CSV file: {file_path}")
    
    temp_file = file_path + '.tmp'  # Temporary file to store amended data
    print(f"Creating a temporary file: {temp_file}")

    try:
        with open(file_path, 'r', encoding='utf-8') as infile, open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
            print(f"Opened {file_path} for reading and {temp_file} for writing.")
            
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Read the header row
            header = next(reader)
            print(f"Original header: {header}")
            
            # Identify the indices of the columns to remove based on the header values
            indices_to_remove = [idx for idx, col in enumerate(header) if col in headers_to_remove]
            print(f"Columns to remove (indices): {indices_to_remove}")
            
            # Create a new header without the columns to remove
            updated_header = [col for idx, col in enumerate(header) if idx not in indices_to_remove]
            writer.writerow(updated_header)
            print(f"Updated header: {updated_header}")
            
            # Process each row and remove the corresponding columns
            for row in reader:
                updated_row = [val for idx, val in enumerate(row) if idx not in indices_to_remove]
                writer.writerow(updated_row)
            print("Processed all rows and removed the specified columns.")
    except Exception as e:
        print(f"Error occurred while processing the CSV: {e}")
        raise

    # Replace the original file with the updated file
    os.replace(temp_file, file_path)
    print(f"Replaced original CSV with updated CSV: {file_path}")    


def scrape_races(races, folder_name, file_name, file_extension, code, file_writer):
    """
    Scrape races and write results to a CSV file with echo commands.
    """
    out_dir = f'../data/{folder_name}/{code}'

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        print(f"Created output directory: {out_dir}")

    file_path = f'{out_dir}/{file_name}.{file_extension}'
    print(f"Starting to scrape races. Output file: {file_path}")

    # Set up retry mechanism
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        with file_writer(file_path) as csv:
            csv.write(settings.csv_header + '\n')
            print("Wrote CSV header.")

            for url in races:
                print(f"Processing race URL: {url}")

                try:
                    r = session.get(url, headers=random_header.header())
                    r.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
                    doc = html.fromstring(r.content)

                    race = Race(url, doc, code, settings.fields)
                    
                    if code == 'flat' and race.race_info['type'] != 'Flat':
                        print(f"Race type '{race.race_info['type']}' does not match 'Flat'. Skipping.")
                        continue
                    elif code == 'jumps' and race.race_info['type'] not in {'Hurdle'}:
                        print(f"Race type '{race.race_info['type']}' does not match 'Hurdle'. Skipping.")
                        continue

                    for row in race.csv_data:
                        csv.write(row + '\n')
                        print(f"Wrote race data to CSV for URL: {url}")

                except requests.exceptions.RequestException as e:
                    print(f"Request error for URL: {url}. Error: {e}")
                    continue
                except VoidRaceError:
                    print(f"VoidRaceError encountered for URL: {url}. Skipping.")
                    continue

                print("Time delay of 3 seconds before next scrape")
                time.sleep(3)

        print("Finished scraping races.")
    except Exception as e:
        print(f"Error occurred during race scraping: {e}")
        raise

    # Call the amend_csv function to modify the CSV after it is generated
  #  amend_csv(file_path)
  #  print("CSV scraping and amendment process complete.")
  #  amend_csv_remove_columns(file_path)
  #  print("CSV remove columns process complete.")



def writer_csv(file_path):
    return open(file_path, 'w', encoding='utf-8')


def writer_gzip(file_path):
    return gzip.open(file_path, 'wt', encoding='utf-8')


def main():
    if settings.toml is None:
        sys.exit()

    if settings.toml['auto_update']:
        check_for_update()

    file_extension = 'csv'
    file_writer = writer_csv

    if settings.toml.get('gzip_output', False):
        file_extension = 'csv.gz'
        file_writer = writer_gzip

    parser = ArgParser()

    if len(sys.argv) > 1:
        args = parser.parse_args(sys.argv[1:])

        if args.date:
            folder_name = 'dates/' + args.region
            file_name = args.date.replace('/', '_')
            races = get_race_urls_date(parser.dates, args.region)
        else:
            folder_name = args.region if args.region else course_name(args.course)
            file_name = args.year
            races = get_race_urls(parser.tracks, parser.years, args.type)

        scrape_races(races, folder_name, file_name, file_extension, args.type, file_writer)
    else:
        if sys.platform == 'linux':
            import readline

            completions = Completer()
            readline.set_completer(completions.complete)
            readline.parse_and_bind('tab: complete')

        while True:
            args = input('[rpscrape]> ').lower().strip()
            args = parser.parse_args_interactive([arg.strip() for arg in args.split()])

            if args:
                if 'dates' in args:
                    races = get_race_urls_date(args['dates'], args['region'])
                else:
                    races = get_race_urls(args['tracks'], args['years'], args['type'])

                scrape_races(
                    races,
                    args['folder_name'],
                    args['file_name'],
                    file_extension,
                    args['type'],
                    file_writer,
                )


if __name__ == '__main__':
    main()
