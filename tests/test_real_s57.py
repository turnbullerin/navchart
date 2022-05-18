import unittest
import pathlib
from navchart.s57 import S57Cell


class TestRealS57s(unittest.TestCase):

    def get_test_file(self, test_file_path):
        p = pathlib.Path(__file__)
        return p.parent / test_file_path

    def test_us1ak90m(self):
        path = self.get_test_file("s57s/US1AK90M/US1AK90M.000")
        cell = S57Cell(path)
        cell._load_updates()

    def test_us2ak5fm(self):
        path = self.get_test_file("s57s/US2AK5FM/US2AK5FM.000")
        cell = S57Cell(path)
        cell._load_updates()

    def test_us3de01m(self):
        path = self.get_test_file("s57s/US3DE01M/US3DE01M.000")
        cell = S57Cell(path)
        cell._load_updates()

    def test_us4ak3sb(self):
        path = self.get_test_file("s57s/US4AK3SB/US4AK3SB.000")
        cell = S57Cell(path)
        cell._load_updates()

    def test_us5ak3mm(self):
        path = self.get_test_file("s57s/US5AK3MM/US5AK3MM.000")
        cell = S57Cell(path)
        cell._load_updates()

    def test_uslgbde(self):
        path = self.get_test_file("s57s/US6LGBDE/US6LGBDE.000")
        cell = S57Cell(path)
        cell._load_updates()
