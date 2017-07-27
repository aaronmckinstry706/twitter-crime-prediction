import datetime
import re

complaint_field_index = {
    'from_date': 1,
    'to_date': 3,
    'lat': 21,
    'lon': 22
}

# Only returns true if complaint is of a crime which occurred within a single day.
def complaint_is_valid(complaint_record):
    if complaint_record[complaint_field_index['from_date']] == complaint_record[complaint_field_index['to_date']] \
            or complaint_record[complaint_field_index['to_date']] == None:
        return True
    return False

def get_complaint_occurrence_day(complaint_record):
    (month, day, year) = [int(n) for n in complaint_record[complaint_field_index['from_date']].split('/')]
    unix_timestamp = int((datetime.datetime(year, month, day) - datetime.datetime(1970, 1, 1)).total_seconds())
    return unix_timestamp / (24*60*60)

tweet_field_index = {'id': 0, 'timestamp': 1, 'postalCode': 2, 'lon': 3, 'lat': 4, 'tweet': 5,
               'user_id': 6, 'application': 7, 'source': 8}

def split_tweet_record(s):
    # type: (str) -> Tuple[int, int, float, float, float, str, int, str, str]
    fields = s.split(',')
    functions = [int, int, float, float, float,
                 lambda x: x, int, lambda x: x, lambda x: x]
    for i in range(len(fields)):
        fields[i] = functions[i](fields[i])
    return tuple(fields)

def tweet_format_is_correct(s):
    return bool(re.match(u"^([^,]+,){8}([^,]+){1}$", s))

def remove_unicode(s):
    return re.sub(u"[^\x00-\x7F]+", u" ", s)

def remove_url(s):
    return re.sub(u"(\s?)http(s?)://[^\s]*(\s?)", u" ", s)

def keep_only_alphanumeric(s):
    return re.sub(u"[^\w\s]", u" ", s)

def remove_apostrophe_in_contractions(s):
    return re.sub(u"(?P<pre>\w+)'(?P<post>[a-zA-Z]+)", u"\g<pre>\g<post>", s)

def strip_excessive_whitespace(s):
    return re.sub(u"\s+", u" ", s).strip()

def get_tweet_modifier(f):
    # type: ((str) -> str) -> (Tuple[int, int, float, float, float, str, int, str, str] -> [int, int, float, float, float, str, int, str, str])
    def modifier(record):
        new_record = [field for field in record]
        new_record[tweet_field_index['tweet']] = f(new_record[tweet_field_index['tweet']])
        return tuple(new_record)
    return modifier

def get_grid_index(grid_boundaries, record, record_type):
    if record_type == 'tweet':
        field_index_table = tweet_field_index
    else:
        field_index_table = complaint_field_index
    
    for i in range(len(grid_boundaries)):
        min_lat, max_lat, min_lon, max_lon = grid_boundaries[i]
        if record[field_index_table['lat']] >= min_lat and record[field_index_table['lat']] < max_lat:
            if record[field_index_table['lon']] >= min_lon and record[field_index_table['lon']] < max_lon:
                return i
    return len(grid_boundaries)
