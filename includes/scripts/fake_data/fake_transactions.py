import json, argparse
from faker import Faker
from random import random, choice, randint

fake = Faker()

class Thing:
    """
    Everything is a 'Thing', right?
    """
    def __init__(self):
        pass
    
    def get_attributes_dict(self) -> dict:
        return vars(self)
    
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
        self.customer = False
    
    def is_customer(self):
        return self.customer
        
class Customer(Person):
    def __init__(self):
        super().__init__()
        self.customer_id = fake.uuid4()
        self.registration_date = fake.date_time_this_decade().isoformat()
        self.current_balance = float(random() * choice([1, -1]) * 1000)
        self.customer = True
        
    def add_amount(self, amount:float)-> None:
        self.current_balance = self.current_balance + amount
    
    def deduct_balance(self, amount:float) -> None:
        self.current_balance = self.current_balance - amount

class Firm(Thing):
    def __init__(self, number_of_branches, number_of_customers):
        super().__init__()
        self.branches = [{'country': fake.country(), 'city': fake.city()} for _ in range(number_of_branches)]
        self.customers = [Customer() for _ in range(number_of_customers)]

    def _get_customers_data(self):
        return [c.get_attributes_dict() for c in self.customers]
    
class Transaction(Thing):
    def __init__(self, sender_id, reciever_id):
        super().__init__()
        self.transaction_id = fake.uuid4()
        self.sender_id = sender_id
        self.receiver_id = reciever_id
        self.transaction_amount = fake.random_number(digits=3)
        self.transaction_currency = 'USD'#fake.currency_code()
        self.transaction_date = fake.date_time_this_year().isoformat()
        self.transaction_type = fake.random_element(elements=("purchase", "transfer"))
        self.transaction_location = fake.city()
        self.device_id = fake.uuid4()

class BankSimulation(Firm):
    def __init__(self, number_of_branches, number_of_customers):
        super().__init__(number_of_branches, number_of_customers)
        self.transactions_data = self._simulate_transactions()
        self.customers_data = self._get_customers_data()

    def _simulate_transactions(self):
        transactions = []
        for _ in self.customers:
            transactions.extend([
                self._do_transaction().get_attributes_dict() for _ in range(randint(1, 3))
                ])
        
        return(transactions)
    
    def _do_transaction(self) -> Transaction:
        scenario    = self._do_transaction_scenario()
        amount      = scenario['amount']
        
        while True:
            sender = self._extract_scenario_characters(scenario, 'from')
            receiver = self._extract_scenario_characters(scenario, 'to')            
            if sender.is_customer() and receiver.is_customer():
                if sender.customer_id != receiver.customer_id:
                    break
            else:
                break
       
        
        if sender.customer:
            sender.deduct_balance(amount)
            sender_id = sender.customer_id
        else:
            sender_id = fake.uuid4()
        
        if receiver.customer:
            receiver.add_amount(amount)
            receiver_id = receiver.customer_id
        else:
            receiver_id = fake.uuid4()
        
        transaction = Transaction(sender_id= sender_id,
                                  reciever_id= receiver_id)
        transaction.transaction_amount = amount
            
        return transaction
    
    def _do_transaction_scenario(self) -> dict[str, str]:
        return choice([
            {'from': 'customer'         , 'to': 'customer'          , 'amount': randint(1, 100)},
            {'from': 'customer'         , 'to': 'non_customer'      , 'amount': randint(1, 100)},
            {'from': 'non_customer'     , 'to': 'customer'          , 'amount': randint(1, 100)},
        ])
        
    def _extract_scenario_characters(self, scenario:dict[str, str], role:str) -> Person:
        if scenario[role] == 'customer':
            return choice(self.customers)
        elif scenario[role] == 'non_customer':
            return Person()



def main(number_of_branches, number_of_customers):
    bank_instance = BankSimulation(number_of_branches, number_of_customers)
    print(json.dumps(bank_instance.transactions_data, indent=4))
    print(json.dumps(bank_instance.customers_data, indent=4))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate bank data with branches and customers.')
    parser.add_argument('number_of_branches', type=int, help='Number of branches')
    parser.add_argument('number_of_customers', type=int, help='Number of customers')

    args = parser.parse_args()
    main(args.number_of_branches, args.number_of_customers)

# how to run?
# /path/to/python /path/to/sparkAirflow/includes/scripts/fake_data/fake_transactions.py 2 2