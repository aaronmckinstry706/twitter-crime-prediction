import unittest

import preprocessing as pp

class PreprocessingTest(unittest.TestCase):
    def test_complaint_is_valid(self):
        self.assertTrue(pp.complaint_is_valid(
            [101109527, u'12/31/2015', u'23:45:00', None, None, u'12/31/2015', 113, u'FORGERY', 729, u'FORGERY,ETC.,UNCLASSIFIED-FELO', u'COMPLETED', u'FELONY', u'N.Y. POLICE DEPT', u'BRONX', 44, u'INSIDE', u'BAR/NIGHT CLUB', None, None, 1007314, 241257, 40.828848333, -73.916661142, u'(40.828848333, -73.916661142)']))
        self.assertTrue(pp.complaint_is_valid(
            [641637920, u'12/31/2015', u'23:25:00', u'12/31/2015', u'23:30:00', u'12/31/2015', 344, u'ASSAULT 3 & RELATED OFFENSES', 101, u'ASSAULT 3', u'COMPLETED', u'MISDEMEANOR', u'N.Y. POLICE DEPT', u'MANHATTAN', 13, u'FRONT OF', u'OTHER', None, None, 987606, 208148, 40.7380024, -73.98789129, u'(40.7380024, -73.98789129)']))
        self.assertFalse(pp.complaint_is_valid(
            [340513307, u'04/06/2015', u'12:00:00', u'04/10/2015', u'18:00:00', u'12/21/2015', 104, u'RAPE', 155, u'RAPE 2', u'COMPLETED', u'FELONY', u'N.Y. POLICE DEPT', u'QUEENS', 110, u'INSIDE', u'OTHER', None, None, None, None, None, None, None]))
        self.assertFalse(pp.complaint_is_valid(
            [None for i in range(max(pp.complaint_field_index.values()) + 1)]))
    
    def test_get_complaint_occurrence_day(self):
        crime_record = [None for i in range(max(pp.complaint_field_index.values()) + 1)]
        crime_record[pp.complaint_field_index['from_date']] = '1/1/1970'
        self.assertEqual(0, pp.get_complaint_occurrence_day(crime_record))
        crime_record[pp.complaint_field_index['from_date']] = '1/2/1970'
        self.assertEqual(1, pp.get_complaint_occurrence_day(crime_record))
    
    def test_split_tweet_record(self):
        self.assertTupleEqual(
            (0, 1, 2, 3.3, 4.4, u"five", 6, u"seven", u"eight"),
            pp.split_tweet_record(
                u"0,1,2,3.3,4.4,five,6,seven,eight"))
    
    def test_tweet_format_is_correct(self):
        self.assertFalse(pp.tweet_format_is_correct("a,3,2,d,2,3,d,3"))
        self.assertFalse(pp.tweet_format_is_correct(u",, ', ;, 2\uacac, f, a, s, d, d, d   "))
        self.assertFalse(pp.tweet_format_is_correct(u" , , ,, , , , , "))
        self.assertTrue(pp.tweet_format_is_correct("a,3,2,d,2,3,d,3,8"))
        self.assertTrue(pp.tweet_format_is_correct(u" ', ;, 2, f,\u7777 a, s, d, d, d"))
        self.assertTrue(pp.tweet_format_is_correct(u" ', ;, 2, f, , ffff, ;23mn., !!!, d"))
    
    def test_remove_unicode(self):
        self.assertEqual(u" keep    my string", 
                         pp.remove_unicode(u"\u342f\u8f73keep\u1212 \u237f my string"))
        self.assertEqual(u"ABC ", pp.remove_unicode(u"ABC\u1738"))
    
    def test_remove_url(self):
        self.assertEqual(u" lkjlihseflkds",
                         pp.remove_url(u" https://www.facebook.com lkjlihseflkds"))
        self.assertEqual(u" 12 htt flihsef",
                         pp.remove_url(u"https://lihsefolihlikhsdf 12 htt flihsef"))
        self.assertEqual(u" 12 htt flihsef https:",
                         pp.remove_url(
                             u"http://lihsefolihlikhsdf 12 htt flihsef https://lihihasef https:"))
    
    def test_keep_only_alphanumeric(self):
        self.assertEqual(u"bla 2398   jh    asdfho8 9824        ",
                         pp.keep_only_alphanumeric(u"bla 2398(*^jh;;;;asdfho8 9824(&^ (*&^"))
    
    def test_remove_apostrophe_in_contractions(self):
        self.assertEqual(u"blasomething yodog HEYOOOO 8287(*& thats rights folks'!'''",
                         pp.remove_apostrophe_in_contractions(
                             u"bla'something yodog HEYOOOO 8287(*& that's right's folks'!'''"))
    
    def test_strip_excessive_whitespace(self):
        self.assertEqual(
            u";lkjasd ;lkj",
            pp.strip_excessive_whitespace(
                u" \t\t\n ;lkjasd \n;lkj   \n  "))
    
    def test_get_grid_index(self):
        grid_bounds = [(0, 1, 0, 1), (0, 1, 1, 2), (1, 2, 0, 1), (1, 2, 1, 2)]
        
        record = [None for i in range(len(pp.tweet_field_index))]
        record[pp.tweet_field_index['lat']] = 0.5
        record[pp.tweet_field_index['lon']] = 1.5
        self.assertEqual(1, pp.get_grid_index(grid_bounds, record, 'tweet'))
        
        record = [None for i in range(max(pp.complaint_field_index.values()) + 1)]
        record[pp.complaint_field_index['lat']] = 0.5
        record[pp.complaint_field_index['lon']] = 1.5
        self.assertEqual(1, pp.get_grid_index(grid_bounds, record, 'complaint'))

if __name__ == '__main__':
    unittest.main()
