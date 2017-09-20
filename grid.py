import bisect
import collections
import math

"""A class which defines a grid square by its upper and lower lat/lon bounds."""
GridSquare = collections.namedtuple('GridSquare', ['lat_min', 'lat_max', 'lon_min', 'lon_max'])

class LatLonGrid(object):
    """
    An object representing a rectangular grid of latitude and longitude regions. The latitude grid
    boundaries are defined by range(lat_min, lat_max + lat_step, lat_step), and the longitude grid
    boundaries are defined similarly.
    """
    def __init__(self, lat_min, lat_max, lon_min, lon_max, lat_step, lon_step):
        self._lat_bounds = tuple(range(lat_min, lat_max + lat_step, lat_step))
        self._lon_bounds = tuple(range(lon_min, lon_max + lon_step, lon_step))
    
    def grid_square_index(self, lat, lon):
        """Given a position defined by (lat, lon), this function returns the index of the grid
        square in which this position lies. If the position lies outside of the grid, then -1 is
        returned."""
        lat_index = bisect.bisect_left(self._lat_bounds, lat)
        lon_index = bisect.bisect_left(self._lon_bounds, lon)
        if lat_index == 0 or lat_index == len(self._lat_bounds) \
                          or lon_index == 0 or lon_index == len(self._lon_bounds):
            return -1
        lat_index = lat_index - 1
        lon_index = lon_index - 1
        return lat_index * self.lat_grid_dimension + lon_index
    
    @property
    def lat_grid_dimension(self):
        return len(self._lat_bounds) - 1
    
    @property
    def lon_grid_dimension(self):
        return len(self._lon_bounds) - 1

def get_longitude_delta(meters_delta, current_lat):
    """At a given latitude, this function calculates how many degrees in longitude
    one would need to change in order to change position by a given number of meters."""
    # current_lat should be in degrees, obv
    earth_radius_meters = 6371008.0 # https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
    meters_delta = float(meters_delta)
    current_lat = float(current_lat)
    return (meters_delta / (earth_radius_meters * math.cos(current_lat / 180.0 * math.pi))) \
        * 180.0 / math.pi

def get_latitude_delta(meters_delta):
    """This function calculates how many degrees in latitude one would need to change in order to
    change position by a given number of meters."""
    earth_radius_meters = 6371008.0 # https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
    meters_delta = float(meters_delta)
    return meters_delta / earth_radius_meters * 180 / math.pi
