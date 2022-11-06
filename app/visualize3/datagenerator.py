import random 
import string 
import time

user_ids = list(range(0, 23))
recipient_ids = list(range(1, 101))

def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def generate_message_new_subs() -> dict:
    random_date_time = str_time_prop("1/1/2008 1:30 PM", "1/1/2009 4:50 AM", '%m/%d/%Y %I:%M %p', random.random())
    random_number = random.choice(user_ids)

    # Generate a random message
    #message = ''.join(random.choice(string.ascii_letters) for i in range(32))

    return {
        'day': random_date_time,
        'new_subscribers':[
            {
                'number': random_number,
                'normal_user': random_number,
                'monthly': random_number,
                'year': random_number
            }
        ]
    }

def generate_message_frequented_tram() -> dict:
    random_date_time = str_time_prop("1/1/2008 1:30 PM", "1/1/2009 4:50 AM", '%m/%d/%Y %I:%M %p', random.random())
    random_number = random.choice(user_ids)

    # Generate a random message
    #message = ''.join(random.choice(string.ascii_letters) for i in range(32))

    return {
        'day': random_date_time,
        'interval_start': random_number,
        'interval_stop': random_number,
        'tram_A':[
            {
                'users': random_number,
                'station1': random_number,
                'station2': random_number,
                'station3': random_number
            }
        ],
        'tram_B':[
            {
                'users': random_number,
                'station1': random_number,
                'station2': random_number,
                'station3': random_number
            }
        ],
        'tram_C':[
            {
                'users': random_number,
                'station1': random_number,
                'station2': random_number,
                'station3': random_number
            }
        ]
    }