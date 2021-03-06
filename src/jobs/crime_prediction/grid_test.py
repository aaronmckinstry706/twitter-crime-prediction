import os
import unittest

from jobs.crime_prediction import grid as grid

class GridSquareTest(unittest.TestCase):
    def setUp(self):
        self.latlongrid = grid.LatLonGrid(
            lat_min=0,
            lat_max=10,
            lat_step=0.9,
            lon_min=0,
            lon_max=11,
            lon_step=0.9)
    
    def test_grid_dimensions(self):
        self.assertEqual(12, self.latlongrid.lat_grid_dimension)
        self.assertEqual(13, self.latlongrid.lon_grid_dimension)
    
    def test_grid_square_index(self):
        self.assertEqual(3*self.latlongrid.lat_grid_dimension + 3,
                         self.latlongrid.grid_square_index(3.5, 3.5))
        self.assertEqual(-1, self.latlongrid.grid_square_index(-0.5, 3.5))
        self.assertEqual(-1, self.latlongrid.grid_square_index(9.0, 11.5))

class GridTest(unittest.TestCase):
    
    def test_get_longitude_delta(self):
        self.assertAlmostEqual(0.0126829387, grid.get_lon_delta(1000, 45), places=4)
    
    def test_git_latitude_delta(self):
        self.assertAlmostEqual(0.00899832896, grid.get_lat_delta(1000), places=4)

if __name__ == '__main__':
    unittest.main()
