import unittest

import clargs

class CLArgsTest(unittest.TestCase):
    def test_process_argv(self):
        bad_argv1 = [None, '12s', 's', '!']
        bad_argv2 = [None, '2015', '13', '42']
        good_argv = [None, '2015', '11', '4']
        self.assertEqual(None, clargs.process_argv(bad_argv1))
        self.assertEqual(None, clargs.process_argv(bad_argv2))
        self.assertTupleEqual((2015, 11, 4), clargs.process_argv(good_argv))

