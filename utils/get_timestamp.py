from datetime import datetime

#######################################################################

def get_timestamp():
    now = datetime.now().astimezone()
    floored = now.replace(second=0, microsecond=0)
    return floored.strftime('%Y-%m-%dT%H:%M:%S.') + '00Z'