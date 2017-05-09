
import csv
import math
import re

def get_longitude_delta(meters_delta, current_latitude):
    """At a given latitude, this function calculates how many degress in longitude
    one would need to change in order to change position by a given number of meters."""
    # current_latitude should be in degrees, obv
    earth_radius_meters = 6371008 # https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
    meters_delta = float(meters_delta)
    current_latitude = float(current_latitude)
    return (meters_delta / (earth_radius_meters * math.cos(current_latitude / 180.0 * math.pi))) \
        * 180.0 / math.pi

def get_grid_block_boundaries(south_west_coordinates, north_east_coordinates, n):
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
    grid_squares = []
    for i in range(len(latitude_boundaries)-1):
        for j in range(len(longitude_boundaries)-1):
            grid_squares.append(
                (latitude_boundaries[i], latitude_boundaries[i+1],
                 longitude_boundaries[j], longitude_boundaries[j+1]))
    return grid_squares

def print_grid_csv(filename, grid_squares):
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
    fields = s.split(',')
    functions = [int, int, float, float, float,
                 lambda x: x, int, lambda x: x, lambda x: x]
    for i in range(len(fields)):
        fields[i] = functions[i](fields[i])
    return fields

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
    TWEET_INDEX = 5
    def modifier(record):
        new_record = [field for field in record]
        new_record[TWEET_INDEX] = f(new_record[TWEET_INDEX])
        return tuple(new_record)
    return modifier
