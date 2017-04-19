import unittest

import run

class RunTest(unittest.TestCase):
    
    def test_get_grid_block_boundaries(self):
        lat_bounds, lon_bounds = get_grid_blocks((0, 0), (10, 10), 10)
        self.assertListEqual([float(i) for i in range(0, 11)],
                             lat_bounds)
        self.assertListEqual([float(i) for i in range(0, 11)],
                             lon_bounds)









