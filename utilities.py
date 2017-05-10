
import csv
import math
import operator
import re

field_index = {'id': 0, 'timestamp': 1, 'postalCode': 2, 'lon': 3, 'lat': 4, 'tweet': 5,
               'user_id': 6, 'application': 7, 'source': 8}

def get_longitude_delta(meters_delta, current_latitude):
    """At a given latitude, this function calculates how many degrees in longitude
    one would need to change in order to change position by a given number of meters."""
    # current_latitude should be in degrees, obv
    earth_radius_meters = 6371008 # https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
    meters_delta = float(meters_delta)
    current_latitude = float(current_latitude)
    return (meters_delta / (earth_radius_meters * math.cos(current_latitude / 180.0 * math.pi))) \
        * 180.0 / math.pi

def get_grid_block_boundaries(south_west_coordinates, north_east_coordinates, n):
    # type: (Tuple[float, float], Tuple[float, float]) -> Tuple[List[float], List[float]]
    south_west_coordinates = [float(x) for x in south_west_coordinates]
    north_east_coordinates = [float(x) for x in north_east_coordinates]
    latitude_boundaries = [south_west_coordinates[0], north_east_coordinates[0]]
    latitude_range = latitude_boundaries[1] - latitude_boundaries[0]
    longitude_boundaries = [south_west_coordinates[1],
                            north_east_coordinates[1]]
    longitude_range = longitude_boundaries[1] - longitude_boundaries[0]
    for i in range(1, n):
        new_latitude_boundary = latitude_boundaries[0] + i*latitude_range/n
        latitude_boundaries.insert(-1, new_latitude_boundary)
        new_longitude_boundary = longitude_boundaries[0] + i*longitude_range/n
        longitude_boundaries.insert(-1, new_longitude_boundary)
    return latitude_boundaries, longitude_boundaries    

def get_grid_square_bounds(latitude_boundaries, longitude_boundaries):
    # type: (List[float], List[float]) -> List[Tuple[float, float, float, float]]
    grid_squares = []
    for i in range(len(latitude_boundaries)-1):
        for j in range(len(longitude_boundaries)-1):
            grid_squares.append(
                (latitude_boundaries[i], latitude_boundaries[i+1],
                 longitude_boundaries[j], longitude_boundaries[j+1]))
    return grid_squares

def print_grid_csv(filename, grid_squares):
    # type: (str, List[Tuple[float, float, float, float]]) -> None
    with open(filename, 'wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        for i in range(len(grid_squares)):
            csv_writer.writerow((i,) + grid_squares[i])

def read_grid_csv(filename):
    with open(filename, 'rb') as csv_file:
        csv_reader = csv.reader(csv_file)
        grid_squares = []
        for row in csv_reader:
            grid_squares.append(tuple(float(num) for num in row[1:]))
        return grid_squares

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

def string_is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def process_argv(argv):
    if len(argv) == 4:
        if reduce(operator.and_, [string_is_int(s) for s in argv[1:]]):
            year = int(argv[1])
            month = int(argv[2])
            day = int(argv[3])
            if 2015 <= year and year <= 2016:
                if 1 <= month and month <= 12:
                    if 1 <= day and day <= 31:
                        return year, month, day
    return None
