from collections import namedtuple
import csv
from pathlib import Path
from os import walk, path, environ, remove
from datetime import datetime

from requests_html import AsyncHTMLSession, HTMLSession
import asyncio


nff_default_url = "https://www.fotball.no/fotballdata/turnering/terminliste/GetAllNationalMatches/"
nff_canonical = "https://www.fotball.no{}"
Tournament = namedtuple('Tournament', 'name id url')
tournaments_default_url = "https://www.fotball.no/fotballdata/turnering/terminliste/?fiksId={}"


def get_matches_links_from_file(tournament):
    if not path.isfile(f"matches/match_links_{tournament.name}.csv"):
        with open(f"matches/match_links_{tournament.name}.csv", 'w') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['date', 'link'])

    matches_links = set()
    with open(f"matches/match_links_{tournament.name}.csv", 'r') as csv_file:
        lines = []
        try:
            lines = [line.split(';')[1] for line in csv_file]
        except IndexError:
            pass

    for line in lines:
        matches_links.add(line)

    return matches_links


async def post_nff_matches(async_session, tournament):
    file_links = get_matches_links_from_file(tournament)
    response = await async_session.post(nff_default_url, {
        'tournamentId': tournament.id
    })
    selector = 'td > a'
    match_links = response.html.find(selector)

    with open("matches/match_links_{}.csv".format(tournament.name), 'w') as csv_file:

        csv_writer = csv.writer(csv_file, delimiter=';')
        csv_writer.writerow(['date', 'link'])
        for link in match_links:
            date_string = link.text
            if '.' in date_string and link.attrs['href'] not in file_links:
                year = link.text.split('.')[-1]
                if int(year) >= 2020:
                    csv_writer.writerow([date_string, nff_canonical.format(link.attrs['href'])])

    return True


def get_tournaments():
    tournaments_list = [
        Tournament('u15', '39910', tournaments_default_url.format('39910')),
        Tournament('u16', '39909', tournaments_default_url.format('39909')),
        Tournament('u17', '39908', tournaments_default_url.format('39908')),
        Tournament('u18', '39907', tournaments_default_url.format('39907')),
        Tournament('u19', '39904', tournaments_default_url.format('39904')),
        Tournament('u20', '39903', tournaments_default_url.format('39903')),
        Tournament('u21', '39901', tournaments_default_url.format('39901')),
    ]
    return tournaments_list

def get_links_for_tournament_from_file(tournament):
    csv_file = None
    matches_links = []
    for (_, _, filenames) in walk('matches'):
        for file in filenames:
            if tournament.name in file:
                csv_file = file

    if csv_file:
        with open('matches/{}'.format(csv_file), 'r') as csv_file:
            lines = [line.split(';')[1] for line in csv_file]
            matches_links = lines[1:]

    return matches_links


def get_tournament_links(async_session, loop, tournaments_list):
    async_tasks = []
    for tournament in tournaments_list:
        print("Getting tournament links for {}".format(tournament.name))
        async_tasks.append(post_nff_matches(async_session, tournament))
    loop.run_until_complete(asyncio.gather(*async_tasks))
    return True


def get_players_names_from_file(tournament):
    names = set()
    if not path.isfile('names/{}.csv'.format(tournament.name)):
        with open('names/{}.csv'.format(tournament.name), 'w') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['name'])
        return names

    with open('names/{}.csv'.format(tournament.name), 'r') as csv_file:
        lines = [line for line in csv_file][1:]

    for line in lines:
        names.add(line.strip('\n'))

    return names


def get_names_for_match(tournament, link, names):
    print(f"Starting to get link {link} for tournament {tournament.name}")
    session = HTMLSession()
    response = session.get(link)
    player_links = response.html.find('a.player-name')
    new_names = set()
    for player_link in player_links:
        name = player_link.text
        if name not in names:
            new_names.add(name)
        else:
            print(f"Name already on the list {name}")
    return new_names


def get_names_for_matches(tournament, links, names):
    new_names_list = set()
    for link in links:
        new_names_list.update(get_names_for_match(
            tournament, link, names))

    for name in new_names_list:
        print(f"New player found {name} for {tournament.name}")
        with open('names/{}.csv'.format(tournament.name), 'a') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([name])


def main():
    tournaments_list = get_tournaments()
    async_session = AsyncHTMLSession()
    loop = asyncio.get_event_loop()
    if not path.exists('matches'):
        Path('matches').mkdir()
        Path('names').mkdir()
    else:
        for (_, _, filenames) in walk('matches'):
            for file in filenames:
                remove('matches/{}'.format(file))

    get_tournament_links(async_session, loop, tournaments_list)

    for tournament in tournaments_list:
        links = get_links_for_tournament_from_file(tournament)
        names = get_players_names_from_file(tournament)
        get_names_for_matches(tournament, links, names)


if __name__ == "__main__":
    main()
