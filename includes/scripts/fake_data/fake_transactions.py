import json, argparse
from faker import Faker
from random import random, choice

fake = Faker()

class Thing:
    """
    Everything is a 'Thing', right?
    """
    def __init__(self):
        pass
    
    def get_attributes_dict(self) -> dict:
        return {key: value for key, value in vars(self)}
    
    def whoami(self) -> str:
        return type(self).__name__
    
class Person(Thing):
    def __init__(self):
        super().__init__()
        self.first_name = fake.first_name()
        self.last_name = fake.last_name()
        self.phone_number = fake.phone_number()
        self.email = f"{self.first_name}.{self.last_name}@{fake.free_email_domain()}".replace(' ', '')
        self.address = fake.address()
        self.date_of_birth = fake.date_of_birth().isoformat()

class Customer(Person):
    def __init__(self):
        super().__init__()
        self.customer_id = fake.uuid4()
        self.registration_date = fake.date_time_this_decade().isoformat()
        self.current_balance = random() * choice([1, -1]) * 100

class Firm(Thing):
    def __init__(self, number_of_branches, number_of_customers):
        super().__init__()
        self.branches = [{'country': fake.country(), 'city': fake.city()} for _ in range(number_of_branches)]
        self.customers = [Customer() for _ in range(number_of_customers)] 

class Transaction(Thing):
    def __init__(self, customer_id):
        super().__init__()
        self.transaction_id = fake.uuid4()
        self.customer_id = customer_id
        self.merchant_id = fake.uuid4()
        self.transaction_amount = fake.random_number(digits=3)
        self.transaction_currency = fake.currency_code()
        self.transaction_date = fake.date_time_this_year().isoformat()
        self.transaction_type = fake.random_element(elements=("purchase", "refund", "transfer"))
        self.transaction_location = fake.city()
        self.device_id = fake.uuid4()

class Bank(Firm):
    def __init__(self, number_of_branches, number_of_customers):
        super().__init__(number_of_branches, number_of_customers)
        self.transactions_data = self._generate_transactions()
        self.customers_data = [c.get_attributes_dict() for c in self.customers]

    def _generate_transactions(self):
        transactions = []
        for customer in self.customers:
            num_transactions = fake.random_int(min=1, max=2)  
            transactions.extend(Transaction(customer.customer_id).get_attributes_dict() for _ in range(num_transactions))
        return transactions

def main(number_of_branches, number_of_customers):
    bank_instance = Bank(number_of_branches, number_of_customers)
    print(json.dumps(bank_instance.transactions_data, indent=2))
    print(json.dumps(bank_instance.customers_data, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate bank data with branches and customers.')
    parser.add_argument('number_of_branches', type=int, help='Number of branches')
    parser.add_argument('number_of_customers', type=int, help='Number of customers')

    args = parser.parse_args()
    main(args.number_of_branches, args.number_of_customers)

# how to run?
# /path/to/python /path/to/sparkAirflow/includes/scripts/fake_data/fake_transactions.py 2 2