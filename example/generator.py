#!/usr/bin/env python3

import random
import requests
import argparse

from faker import Faker

fake = Faker()

url = "http://localhost:8080"

types = ["flat", "room", "house"]
actions = ["buy", "rent", "daily"]
renovations = ["none", "decorating", "european", "design"]
districts = [
    "Akademicheskij", "Alekseevskij", "Altuf'evskij", "Arbat", "Ajeroport", "Babushkinskij", "Basmannyj", "Begovoj",
    "Beskudnikovskij", "Bibirevo", "Birjuljovo Vostochnoe", "Birjuljovo Zapadnoe", "Bogorodskoe", "Brateevo",
    "Butovo Severnoe", "Butovo Juzhnoe", "Butyrskij", "Veshnjaki", "Vnukovo", "Vojkovskij", "Vostochnyj",
    "Vyhino-Zhulebino", "Gagarinskij", "Golovinskij", "Gol'janovo", "Danilovskij", "Degunino Vostochnoe",
    "Degunino Zapadnoe", "Dmitrovskij", "Donskoj", "Dorogomilovo", "Zamoskvorech'e", "Zjuzino", "Zjablikovo",
    "Ivanovskoe", "Izmajlovo Vostochnoe", "Izmajlovo", "Izmajlovo Severnoe", "Kapotnja", "Kon'kovo", "Koptevo",
    "Kosino-Uhtomskij", "Kotlovka", "Krasnosel'skij", "Krylatskoe", "Krjukovo", "Kuz'minki", "Kuncevo", "Kurkino",
    "Levoberezhnyj", "Lefortovo", "Lianozovo", "Lomonosovskij", "Losinoostrovskij", "Ljublino", "Marfino",
    "Marina roshha", "Mar'ino", "Matushkino", "Medvedkovo Severnoe", "Medvedkovo Juzhnoe", "Metrogorodok",
    "Meshhanskij", "Mitino", "Mozhajskij", "Molzhaninovskij", "Moskvorech'e-Saburovo", "Nagatino-Sadovniki",
    "Nagatinskij zaton", "Nagornyj", "Nekrasovka", "Nizhegorodskij", "Novo-Peredelkino", "Novogireevo", "Novokosino",
    "Obruchevskij", "Orehovo-Borisovo Severnoe", "Orehovo-Borisovo Juzhnoe", "Ostankinskij", "Otradnoe",
    "Ochakovo-Matveevskoe", "Perovo", "Pechatniki", "Pokrovskoe-Streshnevo", "Preobrazhenskoe", "Presnenskij",
    "Prospekt Vernadskogo", "Ramenki", "Rostokino", "Rjazanskij", "Savjolki", "Savjolovskij", "Sviblovo", "Severnyj",
    "Silino", "Sokol", "Sokolinaja gora", "Sokol'niki", "Solncevo", "Staroe Krjukovo", "Strogino", "Taganskij",
    "Tverskoj", "Tekstil'shhiki", "Tjoplyj Stan", "Timirjazevskij", "Troparjovo-Nikulino", "Tushino Severnoe",
    "Tushino Juzhnoe", "Filjovskij park", "Fili-Davydkovo", "Hamovniki", "Hovrino", "Horoshjovo-Mnevniki",
    "Horoshjovskij", "Caricyno", "Cherjomushki", "Chertanovo Severnoe", "Chertanovo Central'noe", "Chertanovo Juzhnoe",
    "Shhukino", "Juzhnoportovyj", "Jakimanka", "Jaroslavskij", "Jasenevo"
]


def send_data(json_data):
    r = requests.post(url + "/http", json=json_data, timeout=10)
    r.raise_for_status()
    return r.text


def send_graphql(query):
    request = {"query": query}
    headers = {"schema": "default"}
    r = requests.post(url + "/graphql", json=request, headers=headers, timeout=10)
    r.raise_for_status()

    json_data = r.json()
    if 'errors' in json_data:
        print(json_data['errors'])
    return json_data


def get_agent_from_favihome_com():
    return {
        "home_id": random.randrange(100000),
        "name": fake.name(),
        "phone": "+7" + str(fake.random_number(digits=10, fix_len=True))
    }


def get_agent_from_estate_inc():
    return {
        "company": "estate inc",
        "agent": {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
        }
    }


def get_estate_from_favihome_com(agent_uuid):
    return {
        "home_id": random.randrange(100000),
        "agent_uuid": agent_uuid,
        "action": random.choice(actions),
        "type": random.choice(types),
        "square": random.randrange(300),
        "price": random.randrange(50000) * 1000,
        "address": {
            "country": "Russia",
            "city": "Moscow",
            "metro": random.choice(districts),
            "district": random.choice(districts),
        }
    }


def get_estate_from_estate_inc(agent_uuid):
    return {
        "company": "estate inc",
        "estate": {
            "agent_uuid": agent_uuid,
            "action": random.choice(actions),
            "type": random.choice(types),
            "renovation": random.choice(renovations),
            "price": random.randrange(50000) * 1000,
            "street": fake.street_name(),
            "building": fake.building_number(),
            "district": random.choice(districts),
        }
    }


def generate_data(agents_count=None, estates_count=None, agent_gen=None, estates_gen=None):
    if agent_gen and agents_count:
        for i in range(1, agents_count + 1):
            send_data(agent_gen())
            if 10 <= i <= int(agents_count * 0.9) and i % int(agents_count / 10) == 0:
                print(str(i) + " agents generated")
        print("Total: " + str(agents_count) + " agents generated")

    if estates_gen and estates_count:
        agents = send_graphql("""{
            Agent {
                uuid
                phone
                name
                estates {
                    uuid
                }
            }
        }""")
        for i in range(estates_count):
            agent = random.choice(agents["data"]["Agent"])
            send_data(estates_gen(agent["uuid"]))
            if i != 0 and i % int(estates_count / 10) == 0 and i <= int(estates_count * 0.9):
                print(str(i) + " estates generated")
        print("Total: " + str(estates_count) + " estates generated")


def main():
    parser = argparse.ArgumentParser(description='Generator for agents and estates.')
    parser.add_argument('-a', '--agent', type=int, help='count of agents to generate')
    parser.add_argument('-e', '--estate', type=int, help='count of estates to generate')
    parser.add_argument('-t', '--type', type=int, help='type of generators', default=1)
    args = parser.parse_args()

    agents_count = args.agent
    estates_count = args.estate
    gen_type = args.type
    if not agents_count and not estates_count:
        print('Enter agents count:')
        agents_count = int(input())
        print('Enter estates count:')
        estates_count = int(input())
        print('Enter generators type number:')
        print('1. Favihome.Com')
        print('2. Estate Inc')
        gen_type = int(input())

    if gen_type == 1:
        generate_data(agents_count, estates_count, get_agent_from_favihome_com, get_estate_from_favihome_com)
    elif gen_type == 2:
        generate_data(agents_count, estates_count, get_agent_from_estate_inc, get_estate_from_estate_inc)
    else:
        print("Unknown number of generators type!")


if __name__ == "__main__":
    main()
