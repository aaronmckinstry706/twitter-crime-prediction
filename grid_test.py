import os
import unittest

import grid

class GridTest(unittest.TestCase):
    
    def test_get_grid_block_boundaries(self):
        lat_bounds, lon_bounds = grid.get_grid_block_boundaries(
            (0, 0), (10, 10), 10, 5)
        self.assertListEqual([float(i) for i in range(0, 11)],
                             lat_bounds)
        self.assertListEqual([float(i)*2 for i in range(0, 6)],
                             lon_bounds)
    
    def test_reading_and_printing_grid_square_csv(self):
        grid_squares = [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)]
        grid.print_grid_csv('temp.csv', grid_squares)
        self.assertEqual(
            [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)],
            grid.read_grid_csv('temp.csv'))
        os.remove('temp.csv')

