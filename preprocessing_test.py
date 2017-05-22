import unittest

import preprocessing

class PreprocessingTest(unittest.TestCase):
    
    def test_split_record(self):
        self.assertTupleEqual(
            (0, 1, 2, 3.3, 4.4, u"five", 6, u"seven", u"eight"),
            preprocessing.split_record(
                u"0,1,2,3.3,4.4,five,6,seven,eight"))
    
    def test_format_is_correct(self):
        self.assertFalse(preprocessing.format_is_correct("a,3,2,d,2,3,d,3"))
        self.assertFalse(preprocessing.format_is_correct(u",, ', ;, 2\uacac, f, a, s, d, d, d   "))
        self.assertFalse(preprocessing.format_is_correct(u" , , ,, , , , , "))
        self.assertTrue(preprocessing.format_is_correct("a,3,2,d,2,3,d,3,8"))
        self.assertTrue(preprocessing.format_is_correct(u" ', ;, 2, f,\u7777 a, s, d, d, d"))
        self.assertTrue(preprocessing.format_is_correct(u" ', ;, 2, f, , ffff, ;23mn., !!!, d"))
    
    def test_remove_unicode(self):
        self.assertEqual(u" keep    my string", 
                         preprocessing.remove_unicode(u"\u342f\u8f73keep\u1212 \u237f my string"))
        self.assertEqual(u"ABC ", preprocessing.remove_unicode(u"ABC\u1738"))
    
    def test_remove_url(self):
        self.assertEqual(u" lkjlihseflkds",
                         preprocessing.remove_url(u" https://www.facebook.com lkjlihseflkds"))
        self.assertEqual(u" 12 htt flihsef",
                         preprocessing.remove_url(u"https://lihsefolihlikhsdf 12 htt flihsef"))
        self.assertEqual(u" 12 htt flihsef https:",
                         preprocessing.remove_url(
                             u"http://lihsefolihlikhsdf 12 htt flihsef https://lihihasef https:"))
    
    def test_keep_only_alphanumeric(self):
        self.assertEqual(u"bla 2398   jh    asdfho8 9824        ",
                         preprocessing.keep_only_alphanumeric(u"bla 2398(*^jh;;;;asdfho8 9824(&^ (*&^"))
    
    def test_remove_apostrophe_in_contractions(self):
        self.assertEqual(u"blasomething yodog HEYOOOO 8287(*& thats rights folks'!'''",
                         preprocessing.remove_apostrophe_in_contractions(
                             u"bla'something yodog HEYOOOO 8287(*& that's right's folks'!'''"))
    
    def test_strip_excessive_whitespace(self):
        self.assertEqual(
            u";lkjasd ;lkj",
            preprocessing.strip_excessive_whitespace(
                u" \t\t\n ;lkjasd \n;lkj   \n  "))
    
    def test_get_grid_index(self):
        grid_bounds = [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)]
        record = [None for i in range(len(preprocessing.field_index))]
        record[preprocessing.field_index['lat']] = 0.5
        record[preprocessing.field_index['lon']] = 1.5
        self.assertEqual(1, preprocessing.get_grid_index(grid_bounds, record))

