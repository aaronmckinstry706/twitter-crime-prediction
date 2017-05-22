import re

field_index = {'id': 0, 'timestamp': 1, 'postalCode': 2, 'lon': 3, 'lat': 4, 'tweet': 5,
               'user_id': 6, 'application': 7, 'source': 8}

def split_record(s):
    # type: (str) -> Tuple[int, int, float, float, float, str, int, str, str]
    fields = s.split(',')
    functions = [int, int, float, float, float,
                 lambda x: x, int, lambda x: x, lambda x: x]
    for i in range(len(fields)):
        fields[i] = functions[i](fields[i])
    return tuple(fields)

def format_is_correct(s):
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
        new_record[field_index['tweet']] = f(new_record[field_index['tweet']])
        return tuple(new_record)
    return modifier

def get_grid_index(grid_boundaries, record):
    for i in range(len(grid_boundaries)):
        min_lat, max_lat, min_lon, max_lon = grid_boundaries[i]
        if record[field_index['lat']] >= min_lat and record[field_index['lat']] < max_lat:
            if record[field_index['lon']] >= min_lon and record[field_index['lon']] < max_lon:
                return i
    return len(grid_boundaries)
