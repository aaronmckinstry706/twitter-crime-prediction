import os
import unittest


import utilities

class UtilitiesTest(unittest.TestCase):
    
    def test_get_grid_block_boundaries(self):
        lat_bounds, lon_bounds = utilities.get_grid_block_boundaries(
            (0, 0), (10, 10), 10, 5)
        self.assertListEqual([float(i) for i in range(0, 11)],
                             lat_bounds)
        self.assertListEqual([float(i)*2 for i in range(0, 6)],
                             lon_bounds)
    
    def test_reading_and_printing_grid_square_csv(self):
        grid_squares = [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)]
        utilities.print_grid_csv('temp.csv', grid_squares)
        self.assertEqual(
            [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)],
            utilities.read_grid_csv('temp.csv'))
        os.remove('temp.csv')
    
    def test_split_record(self):
        self.assertTupleEqual(
            (0, 1, 2, 3.3, 4.4, u"five", 6, u"seven", u"eight"),
            utilities.split_record(
                u"0,1,2,3.3,4.4,five,6,seven,eight"))
    
    def test_get_longitude_delta(self):
        self.assertAlmostEqual(0.0126829387, utilities.get_longitude_delta(1000, 45), places=4)
    
    def test_format_is_correct(self):
        self.assertFalse(utilities.format_is_correct("a,3,2,d,2,3,d,3"))
        self.assertFalse(utilities.format_is_correct(u",, ', ;, 2\uacac, f, a, s, d, d, d   "))
        self.assertFalse(utilities.format_is_correct(u" , , ,, , , , , "))
        self.assertTrue(utilities.format_is_correct("a,3,2,d,2,3,d,3,8"))
        self.assertTrue(utilities.format_is_correct(u" ', ;, 2, f,\u7777 a, s, d, d, d"))
        self.assertTrue(utilities.format_is_correct(u" ', ;, 2, f, , ffff, ;23mn., !!!, d"))
    
    def test_remove_unicode(self):
        self.assertEqual(u" keep    my string", 
                         utilities.remove_unicode(u"\u342f\u8f73keep\u1212 \u237f my string"))
        self.assertEqual(u"ABC ", utilities.remove_unicode(u"ABC\u1738"))
    
    def test_remove_url(self):
        self.assertEqual(u" lkjlihseflkds",
                         utilities.remove_url(u" https://www.facebook.com lkjlihseflkds"))
        self.assertEqual(u" 12 htt flihsef",
                         utilities.remove_url(u"https://lihsefolihlikhsdf 12 htt flihsef"))
        self.assertEqual(u" 12 htt flihsef https:",
                         utilities.remove_url(
                             u"http://lihsefolihlikhsdf 12 htt flihsef https://lihihasef https:"))
    
    def test_keep_only_alphanumeric(self):
        self.assertEqual(u"bla 2398   jh    asdfho8 9824        ",
                         utilities.keep_only_alphanumeric(u"bla 2398(*^jh;;;;asdfho8 9824(&^ (*&^"))
    
    def test_remove_apostrophe_in_contractions(self):
        self.assertEqual(u"blasomething yodog HEYOOOO 8287(*& thats rights folks'!'''",
                         utilities.remove_apostrophe_in_contractions(
                             u"bla'something yodog HEYOOOO 8287(*& that's right's folks'!'''"))
    
    def test_get_grid_square_bounds(self):
        self.assertEqual(
            [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)],
            utilities.get_grid_square_bounds(range(3), range(3)))
    
    def test_strip_excessive_whitespace(self):
        self.assertEqual(
            u";lkjasd ;lkj",
            utilities.strip_excessive_whitespace(
                u" \t\t\n ;lkjasd \n;lkj   \n  "))
    
    def test_get_grid_index(self):
        grid_bounds = [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)]
        record = [None for i in range(len(utilities.field_index))]
        record[utilities.field_index['lat']] = 0.5
        record[utilities.field_index['lon']] = 1.5
        self.assertEqual(1, utilities.get_grid_index(grid_bounds, record))
    
    def test_process_argv(self):
        bad_argv1 = [None, '12s', 's', '!']
        bad_argv2 = [None, '2015', '13', '42']
        good_argv = [None, '2015', '11', '4']
        self.assertEqual(None, utilities.process_argv(bad_argv1))
        self.assertEqual(None, utilities.process_argv(bad_argv2))
        self.assertTupleEqual((2015, 11, 4), utilities.process_argv(good_argv))

if __name__ == '__main__':
    unittest.main()

