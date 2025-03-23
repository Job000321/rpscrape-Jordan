#!/usr/bin/env python3
import requests
import sys
from datetime import datetime, timedelta
from lxml import html
from orjson import loads
from utils.header import RandomHeader
from utils.lxml_funcs import find

random_header = RandomHeader()

def clean_name(name):
    if name:
        return name.strip().replace("'", '').lower().title()
    else:
        return ''

def get_race_urls(session, racecard_url):
    r = session.get(racecard_url, headers=random_header.header())
    doc = html.fromstring(r.content)

    race_urls = []

    for meeting in doc.xpath('//section[@data-accordion-row]'):
        course = meeting.xpath(".//span[contains(@class, 'RC-accordion__courseName')]")[0]
        if valid_course(course.text_content().strip().lower()):
            for race in meeting.xpath(".//a[@class='RC-meetingItem__link js-navigate-url']"):
                race_urls.append('https://www.racingpost.com' + race.attrib['href'])

    return sorted(list(set(race_urls)))

def valid_course(course):
    invalid = ['free to air', 'worldwide stakes', '(arab)']
    return all([x not in course for x in invalid])

def get_runners(session, profile_urls):
    runners = []

    for url in profile_urls:
        r = session.get(url, headers=random_header.header())
        doc = html.fromstring(r.content)

        try:
            json_str = (
                doc.xpath('//body/script')[0]
                .text.split('window.PRELOADED_STATE =')[1]
                .split('\n')[0]
                .strip()
                .strip(';')
            )
            js = loads(json_str)
            horse_name = clean_name(js['profile']['horseName'])
            runners.append(horse_name)
        except (IndexError, KeyError):
            continue

    return runners

def generate_sql(race_name, horse_names):
    names = "', '".join(horse_names)
    # print(names)
    return f"-- SQL for race: {race_name}\nSELECT * from race_results where horse in ('{names}');"

def parse_races(session, race_urls):
    race_sql_statements = []

    for url in race_urls:
        r = session.get(url, headers=random_header.header())
        if r.status_code != 200:
            continue

        try:
            doc = html.fromstring(r.content)
        except Exception:
            continue

        race_name = find(doc, 'span', 'RC-header__raceInstanceTitle') or "Unnamed Race"
        profile_hrefs = doc.xpath("//a[@data-test-selector='RC-cardPage-runnerName']/@href")
        profile_urls = ['https://www.racingpost.com' + a.split('#')[0] + '/form' for a in profile_hrefs]

        horse_names = get_runners(session, profile_urls)
        if horse_names:
            race_sql = generate_sql(race_name, horse_names)
            race_sql_statements.append(race_sql)

    return race_sql_statements

def main():
    if len(sys.argv) != 2 or sys.argv[1].lower() not in {'today', 'tomorrow'}:
        return print('Usage: ./horse_name_selector.py [today|tomorrow]')

    racecard_url = 'https://www.racingpost.com/racecards'

    if sys.argv[1].lower() == 'today':
        date = datetime.today().strftime('%Y-%m-%d')
    else:
        date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
        racecard_url += '/tomorrow'

    session = requests.Session()
    race_urls = get_race_urls(session, racecard_url)
    race_sql_statements = parse_races(session, race_urls)

    for sql in race_sql_statements:
        print(sql + "\n")

if __name__ == '__main__':
    main()

