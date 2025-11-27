import logging
from utils.get_timestamp import get_timestamp

#######################################################################

def update_env(env_path='.env'):
    timestamp = get_timestamp()
    new_line = f'''LAST_READ='{timestamp}'\n'''

    with open('.env', 'r') as f:
        lines = f.readlines()

    with open('.env', 'w') as f:
        for line in lines:
            if line.startswith('LAST_READ='):
                f.write(new_line)
            else:
                f.write(line)
    logging.info('LAST_READ updated')
    return