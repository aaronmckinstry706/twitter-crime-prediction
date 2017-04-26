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
    
    def test_split_record(self):
        self.assertListEqual(
            [0, 1, 2, 3.3, 4.4, u"five", 6, u"seven", u"eight"],
            utilities.split_record(
                u"0,1,2,3.3,4.4,five,6,seven,eight"))

if __name__ == '__main__':
    unittest.main()







