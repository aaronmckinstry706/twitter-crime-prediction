import csv
import math
import os

def get_longitude_delta(meters_delta, current_latitude):
    """At a given latitude, this function calculates how many degrees in longitude
    one would need to change in order to change position by a given number of meters."""
    # current_latitude should be in degrees, obv
    earth_radius_meters = 6371008 # https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
    meters_delta = float(meters_delta)
    current_latitude = float(current_latitude)
    return (meters_delta / (earth_radius_meters * math.cos(current_latitude / 180.0 * math.pi))) \
        * 180.0 / math.pi

def get_grid_block_boundaries(south_west_coordinates, north_east_coordinates, n_lat, n_lon):
    # type: (Tuple[float, float], Tuple[float, float]) -> Tuple[List[float], List[float]]
    south_west_coordinates = [float(x) for x in south_west_coordinates]
    north_east_coordinates = [float(x) for x in north_east_coordinates]
    latitude_boundaries = [south_west_coordinates[0], north_east_coordinates[0]]
    latitude_range = latitude_boundaries[1] - latitude_boundaries[0]
    longitude_boundaries = [south_west_coordinates[1],
                            north_east_coordinates[1]]
    longitude_range = longitude_boundaries[1] - longitude_boundaries[0]
    for i in range(1, n_lat):
        new_latitude_boundary = latitude_boundaries[0] + i*latitude_range/n_lat
        latitude_boundaries.insert(-1, new_latitude_boundary)
    for i in range(1, n_lon):
        new_longitude_boundary = longitude_boundaries[0] + i*longitude_range/n_lon
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

