import os
import unittest

import grid

class GridSquareTest(unittest.TestCase):
    def setUp(self):
        self.latlongrid = grid.LatLonGrid(
            lat_min=0,
            lat_max=10,
            lat_step=1,
            lon_min=0,
            lon_max=11,
            lon_step=1)
    
    def test_grid_dimensions(self):
        self.assertEqual(10, self.latlongrid.lat_grid_dimension)
        self.assertEqual(11, self.latlongrid.lon_grid_dimension)
    
    def test_grid_square_index(self):
        self.assertEqual(3*10 + 3, self.latlongrid.grid_square_index(3.5, 3.5))
        self.assertEqual(-1, self.latlongrid.grid_square_index(-0.5, 3.5))
        self.assertEqual(-1, self.latlongrid.grid_square_index(9.0, 11.5))

class GridTest(unittest.TestCase):
    
    def test_get_longitude_delta(self):
        self.assertAlmostEqual(0.0126829387, grid.get_longitude_delta(1000, 45), places=4)
    
    def test_git_latitude_delta(self):
        self.assertAlmostEqual(0.00899832896, grid.get_latitude_delta(1000), places=4)

if __name__ == '__main__':
    unittest.main()