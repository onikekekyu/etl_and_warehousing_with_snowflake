import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

from dotenv import load_dotenv
from faker import Faker
from datetime import date, datetime

load_dotenv()
fake = Faker()
inventory = [
    "Statue Kirsten Policier Gold", "Statue Kirsten Policier Silver", "Statue Kirsten Policier Regular",
    "Statue Kirsten Vampire Gold", "Statue Kirsten Vampire Silver", "Statue Kirsten Vampire Regular",
    "Statue Kirsten Super-heros Gold", "Statue Kirsten Super-heros Silver", "Statue Kirsten Super-heros Regular",
    "Statue Kirsten Pirate Gold", "Statue Kirsten Pirate Silver", "Statue Kirsten Pirate Regular",
    "Statue Kirsten Astronaute Gold", "Statue Kirsten Astronaute Silver", "Statue Kirsten Astronaute Regular",
    "Statue Matteo Policier Gold", "Statue Matteo Policier Silver", "Statue Matteo Policier Regular",
    "Statue Matteo Vampire Gold", "Statue Matteo Vampire Silver", "Statue Matteo Vampire Regular",
    "Statue Matteo Super-heros Gold", "Statue Matteo Super-heros Silver", "Statue Matteo Super-heros Regular",
    "Statue Matteo Pirate Gold", "Statue Matteo Pirate Silver", "Statue Matteo Pirate Regular",
    "Statue Matteo Astronaute Gold", "Statue Matteo Astronaute Silver", "Statue Matteo Astronaute Regular",
    "Statue Sasha Policier Gold", "Statue Sasha Policier Silver", "Statue Sasha Policier Regular",
    "Statue Sasha Vampire Gold", "Statue Sasha Vampire Silver", "Statue Sasha Vampire Regular",
    "Statue Sasha Super-heros Gold", "Statue Sasha Super-heros Silver", "Statue Sasha Super-heros Regular",
    "Statue Sasha Pirate Gold", "Statue Sasha Pirate Silver", "Statue Sasha Pirate Regular",
    "Statue Sasha Astronaute Gold", "Statue Sasha Astronaute Silver", "Statue Sasha Astronaute Regular",
    "Statue Corentin Policier Gold", "Statue Corentin Policier Silver", "Statue Corentin Policier Regular",
    "Statue Corentin Vampire Gold", "Statue Corentin Vampire Silver", "Statue Corentin Vampire Regular",
    "Statue Corentin Super-heros Gold", "Statue Corentin Super-heros Silver", "Statue Corentin Super-heros Regular",
    "Statue Corentin Pirate Gold", "Statue Corentin Pirate Silver", "Statue Corentin Pirate Regular",
    "Statue Corentin Astronaute Gold", "Statue Corentin Astronaute Silver", "Statue Corentin Astronaute Regular", "Statue Corentin Nu Gold"
]

def print_client_support():
    global inventory, fake
    state = fake.state_abbr()
    client_support = {'txid': str(uuid.uuid4()),
                      'rfid': hex(random.getrandbits(96)),
                      'item': fake.random_element(elements=inventory),
                      'purchase_time': datetime.utcnow().isoformat(),
                      'expiration_time': date(2023, 6, 1).isoformat(),
                      'days': fake.random_int(min=1, max=7),
                      'name': fake.name(),
                      'address': fake.none_or({'street_address': fake.street_address(), 
                                                'city': fake.city(), 'state': state, 
                                                'postalcode': fake.postalcode_in_state(state)}),
                      'phone': fake.none_or(fake.phone_number()),
                      'email': fake.none_or(fake.email()),
                      'emergency_contact' : fake.none_or({'name': fake.name(), 'phone': fake.phone_number()}),
    }
    d = json.dumps(client_support) + '\n'
    sys.stdout.write(d)


if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_client_support()
    print('')