import bisect
import collections
import math

"""A class which defines a square by its upper and lower lat/lon bounds."""
LatLonSquare = collections.namedtuple('Square', ['lat_min', 'lat_max', 'lon_min', 'lon_max'])

class LatLonGrid(object):
    """
    An object representing a rectangular grid of latitude and longitude regions. The latitude
    boundaries are placed at even intervals of lat_step until lat_max is passed or reached. The
    final latitude boundary is then lat_max. The longitude boundaries are placed similarly. 
    
    Edge case, for clarification: LatLonGrid(0, 1, 0.15, ...) will have latitude boundaries of
    approximately [0, 0.15, 0.3, 0.45, 0.6, 0.75, 0.9, 1.0]. LatLonGrid(0, 1, 0.5) will have
    latitude boundaries of [0, 0.5, 1.0]. The same holds for longitude boundaries when the
    longitude max, min and step are all defined as in the two examples given. 
    """
    def __init__(self, lat_min, lat_max, lon_min, lon_max, lat_step, lon_step):
        # Make sure all args are floats.
        lat_min = float(lat_min)
        lat_max = float(lat_max)
        lon_min = float(lon_min)
        lon_max = float(lon_max)
        lat_step = float(lat_step)
        lon_step = float(lon_step)
        
        num_lat_bounds = (lat_max - lat_min) / lat_step
        self._lat_bounds = []
        for b in range(0, int(num_lat_bounds) + 1, 1):
            self._lat_bounds.append((lat_max - lat_min) * (b / num_lat_bounds) + lat_min)
        self._lat_bounds.append(lat_max)
        self._lat_bounds = tuple(self._lat_bounds)
        
        num_lon_bounds = (lon_max - lon_min) / lon_step
        self._lon_bounds = []
        for b in range(0, int(num_lon_bounds) + 1, 1):
            self._lon_bounds.append((lon_max - lon_min) * (b / num_lon_bounds) + lon_min)
        self._lon_bounds.append(lon_max)
        self._lon_bounds = tuple(self._lon_bounds)
    
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

def get_lon_delta(meters_delta, current_lat):
    """At a given latitude, this function calculates how many degrees in longitude
    one would need to change in order to change position by a given number of meters."""
    # current_lat should be in degrees, obv
    earth_radius_meters = 6371008.0 # https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
    meters_delta = float(meters_delta)
    current_lat = float(current_lat)
    return (meters_delta / (earth_radius_meters * math.cos(current_lat / 180.0 * math.pi))) \
        * 180.0 / math.pi

def get_lat_delta(meters_delta):
    """This function calculates how many degrees in latitude one would need to change in order to
    change position by a given number of meters."""
    earth_radius_meters = 6371008.0 # https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html
    meters_delta = float(meters_delta)
    return meters_delta / earth_radius_meters * 180 / math.pi
