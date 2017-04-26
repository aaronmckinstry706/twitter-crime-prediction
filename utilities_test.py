import unittest

import utilities

class UtilitiesTest(unittest.TestCase):
    
    def test_get_grid_block_boundaries(self):
        lat_bounds, lon_bounds = utilities.get_grid_block_boundaries(
            (0, 0), (10, 10), 10)
        self.assertListEqual([float(i) for i in range(0, 11)],
                             lat_bounds)
        self.assertListEqual([float(i) for i in range(0, 11)],
                             lon_bounds)

if __name__ == '__main__':
    unittest.main()







