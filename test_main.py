import unittest
from main import FishMarket


class TestFishMarket(unittest.TestCase):

    def setUp(self):
        self.fish_market = FishMarket()

    def test_read_from_s3(self):
        self.assertEqual(self.fish_market.read_from_s3(), ['python/fish-market-mon.csv',
                                                           'python/fish-market-tues.csv',
                                                           'python/fish-market.csv'])

if __name__ == '__main__':
    unittest.main()
