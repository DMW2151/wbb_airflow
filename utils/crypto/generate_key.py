import os
from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key().decode()
with open(os.path.join('./envs/wbb_fermet_airflow.env'), 'w+') as fi:
    fi.write(f'AIRFLOW__CORE__FERNET_KEY={fernet_key}')
