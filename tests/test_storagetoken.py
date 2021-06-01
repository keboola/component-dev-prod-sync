'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
from freezegun import freeze_time

from component import StorageToken


class TestComponent(unittest.TestCase):

    @freeze_time("2021-06-01T10:15:28+0200")
    def test_expired_token_short(self):
        token = {'id': '9259',
                 'expires': '2021-06-01T10:20:28+0200',
                 'token': 'XXXXXXX'}
        token_inst = StorageToken(token['id'], token['token'], token['expires'])

        self.assertTrue(token_inst.is_expired())

    @freeze_time("2021-06-01T10:15:28+0200")
    def test_expired_token_long(self):
        token = {'id': '9259',
                 'expires': '2021-06-01T09:20:28+0200',
                 'token': 'XXXXXXX'}
        token_inst = StorageToken(token['id'], token['token'], token['expires'])

        self.assertTrue(token_inst.is_expired())

    @freeze_time("2021-06-01T10:15:28+0200")
    def test_nonexpired_token(self):
        token = {'id': '9259',
                 'expires': '2021-06-01T10:35:28+0200',
                 'token': 'XXXXXXX'}
        token_inst = StorageToken(token['id'], token['token'], token['expires'])

        self.assertFalse(token_inst.is_expired())


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
