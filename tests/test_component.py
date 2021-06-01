'''
Created on 12. 11. 2018

@author: esner
'''
import mock
import os
import unittest
from freezegun import freeze_time

from component import Component


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_find_secrets(self):
        source_cfg = {'configuration': {'parameters': {'api': {'baseUrl': ''},
                                                       'config': {'nonsecret': 'sss',
                                                                  '#secret1': 1234},
                                                       '#secret2': 123456,
                                                       "nested": {"flat": "",
                                                                  "l2": {"flat": "", "#secret3": 123456789}}},
                                        }}
        expected = ['config.#secret1', '#secret2', "nested.l2.#secret3"]
        encrypted = Component._retrieve_encrypted_properties(source_cfg)
        self.assertEqual(expected, encrypted)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
