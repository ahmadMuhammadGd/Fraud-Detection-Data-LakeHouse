from faker import Faker
from random import random, choice, randint
from datetime import timedelta, datetime
import json 

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
            
    def is_customer(self):
        return False
        
class Customer(Person):
    def __init__(self):
        super().__init__()
        self.customer_id = fake.uuid4()
        self.registration_datetime = fake.date_time_this_decade().isoformat()
        self.current_balance = float(random() * choice([1, -1]) * 1000)
        
    def add_amount(self, amount:float)-> None:
        self.current_balance = self.current_balance + amount
    
    def deduct_balance(self, amount:float) -> None:
        self.current_balance = self.current_balance - amount
    
    def is_customer(self):
        return True

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
        self.transaction_datetime = fake.date_time_this_year().isoformat()
        self.transaction_type = fake.random_element(elements=("purchase", "transfer"))
        self.transaction_location = fake.city()
        self.device_id = fake.uuid4()

class BankAppSimulation(Firm):
    def __init__(self, number_of_branches, number_of_customers):
        super().__init__(number_of_branches, number_of_customers)
        self.app_transactions_data = []
        self.customers_data = self._get_customers_data()
        self.fraud_transactions_data=[]

    def simulate(self, fraud_probability:float=0.2, incomming_customers_probability=0.1):
        if random() < fraud_probability:
            self._add_random_fraud_flag()
        
        if random() < incomming_customers_probability:
            self._add_customer()
            
        new_transactions= []
        new_transactions.extend([
            self._do_transaction().get_attributes_dict() for _ in range(randint(1, 3))
        ])
        self.app_transactions_data = new_transactions
        
        return (new_transactions)
    
    def _add_customer(self):
        self.customers.append(Customer())
        
    def _add_random_fraud_flag(self)->dict:
        if len(self.app_transactions_data) <= 1:
            return 
        
        random_transaction = choice(self.app_transactions_data)
        transaction_date = datetime.fromisoformat(random_transaction['transaction_datetime'])  # Convert ISO format string to datetime
        max_end_date = transaction_date + timedelta(days=3)

        fraud_transaction = {
            'transaction_id'    : random_transaction['transaction_id'],
            'labeled_at'        : fake.date_between(start_date= transaction_date, 
                                            end_date= max_end_date).isoformat()
        }
        
        self.fraud_transactions_data.append(fraud_transaction)
        return fraud_transaction

    def _do_transaction(self) -> Transaction:
        scenario = self._do_transaction_scenario()
        amount = scenario['amount']
            
        while True:
                sender = self._extract_scenario_characters(scenario, 'from')
                receiver = self._extract_scenario_characters(scenario, 'to')            
                if sender.is_customer() and receiver.is_customer():
                    if sender.customer_id != receiver.customer_id:
                        break
                else:
                    break
       
        if sender.is_customer():
            sender.deduct_balance(amount)
            sender_id = sender.customer_id
        else:
            sender_id = fake.uuid4()
        
        if receiver.is_customer():
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
        
        
# # test
# b = BankAppSimulation(3, 3)
# print('customer_num_init:\t\t', len(b.customers))

# for _ in range(200):
#     b.simulate()


# print('customer_num_end:\t\t', len(b.customers))
# print('customer_data_num_end:\t\t', len(b.customers_data))
# print(json.dumps(b.fraud_transactions_data, indent=4))