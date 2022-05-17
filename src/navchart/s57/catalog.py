import functools
import zlib
import logging
from pathlib import Path
import enum
import decimal

import s57
from iso8211_scratch import ISO8211File, Record


class Implementation(enum.Enum):

    TEXT = 'TXT'
    ASCII = 'ASC'
    TIF = 'TIF'
    BINARY = 'BIN'


class Entry:

    def __init__(self):
        self.enc_root = None
        self.metadata = None
        self.identifier = None

    @functools.cached_property
    def path(self):
        return self.enc_root / self.relative_path

    @functools.cached_property
    def relative_path(self):
        return Path(self.metadata["FILE"])

    @functools.cached_property
    def name(self):
        return self.relative_path.name

    @functools.cached_property
    def long_file(self):
        return self.metadata["LFIL"]

    @functools.cached_property
    def east(self):
        return self.metadata["ELON"]

    @functools.cached_property
    def west(self):
        return self.metadata["WLON"]

    @functools.cached_property
    def north(self):
        return self.metadata["NLAT"]

    @functools.cached_property
    def south(self):
        return self.metadata["SLAT"]

    @functools.cached_property
    def crc(self):
        return self.metadata["CRCS"]

    @functools.cached_property
    def comment(self):
        return self.metadata["COMT"]

    @functools.cached_property
    def volume(self):
        return int(self.metadata["VOLM"][1:3]), int(self.metadata["VOLM"][4:6])

    @functools.cached_property
    def real_crc(self):
        with open(self.path, "rb") as h:
            real_crc = hex((zlib.crc32(h.read()) & 0xffffffff)).upper()[2:]
            while len(real_crc) < 8:
                real_crc = "0" + real_crc
            return real_crc

    def check_crc(self):
        return self.real_crc == self.crc

    @functools.cached_property
    def encoding(self):
        if self.implementation == Implementation.BINARY:
            return "bin"
        if self.implementation == Implementation.TIF:
            return "tif"
        work = ["ascii", "iso-8859-1", "ucs-2", "utf-8-sig", "windows-1252", "utf-16", "utf-32"]
        with open(self.path, "rb") as h:
            data = h.read()
            for enc in work:
                try:
                    data.decode(enc)
                    return enc
                except UnicodeError:
                    pass
        return None

    def to_cell(self):
        if self.name.endswith(".000"):
            return s57.S57Cell(self.path)
        return None

    @functools.cached_property
    def implementation(self):
        if self.metadata["IMPL"] == "BIN":
            return Implementation.BINARY
        elif self.metadata["IMPL"] == "ASC":
            return Implementation.ASCII
        elif self.metadata["IMPL"] == "TXT":
            return Implementation.TEXT
        elif self.metadata["IMPL"] == "TIF":
            return Implementation.TIF
        raise ValueError("Unsupported implementation value: {}".format(self.metadata["IMPL"]))

    def from_iso8211(self, catalog_file, dataset: Record):
        self.enc_root = catalog_file.enc_root
        self.identifier = dataset["ITEM0"]
        self.metadata = dataset["CATD"]
        return self

    def from_path(self, enc_root, path, long_file, south, west, north, east, volume=1, max_volume=1, comment=""):
        self.identifier = None
        self.enc_root = enc_root
        rel_path = path
        if rel_path.startswith(self.enc_root):
            rel_path = path[len(self.enc_root):]
        self.metadata = {
            "FILE": rel_path,
            "COMT": comment,
            "ELON": decimal.Decimal(east),
            "NLAT": decimal.Decimal(north),
            "SLAT": decimal.Decimal(south),
            "WLONG": decimal.Decimal(west),
            "LFIL": long_file,
            "VOLM": "V{:02d}X{:02d}".format(volume, max_volume)
        }
        if path.endswith("CATALOG.031"):
            self.metadata["IMPL"] = "ASC"
        elif path.endswith(".TXT"):
            self.metadata["IMPL"] = "TXT"
        elif path.endswith(".TIF"):
            self.metadata["IMPL"] = "TIF"
        else:
            self.metadata["IMPL"] = "BIN"
        return self

    def from_entry(self, entry, new_root=None, new_sub_dir=None):
        self.identifier = None
        self.metadata = entry.metadata
        self.enc_root = entry.enc_root
        if new_root:
            self.enc_root = new_root
        if new_sub_dir is not None:
            if new_sub_dir:
                self.metadata["FILE"] = Path(new_sub_dir) / self.name
            else:
                self.metadata["FILE"] = self.name
        return self


class S57Catalog:

    def __init__(self):
        self._raw = None
        self.path = None
        self.files = {}
        self._iter_keys = []

    def from_file(self, path):
        self.path = Path(path)
        self.files = {}
        self._raw = ISO8211File()
        self._raw.from_file(path)
        for dataset in self._raw.datasets():
            entry = Entry().from_iso8211(self, dataset)
            if entry.name in self.files:
                logging.getLogger(__name__).warning(
                    "Duplicate entry for {} ({} and {}), keeping only the most recent".format(
                        entry.name,
                        self.files[entry.name].relative_path,
                        entry.relative_path
                    )
                )
            self.files[entry.name] = entry
        return self

    def from_scratch(self, enc_root):
        self.path = Path(enc_root).absolute() / "CATALOG.031"
        return self

    def append(self, entry: Entry):
        if entry.name in self.files:
            logging.getLogger(__name__).warning(
                "Duplicate entry for {} ({} and {}), keeping only the most recent".format(
                    entry.name,
                    self.files[entry.name].relative_path,
                    entry.relative_path
                )
            )
            entry.identifier = self.files[entry.name].identifier
        else:
            entry.identifier = len(self.files) + 1
        self.files[entry.name] = entry

    def merge(self, catalog, skip_duplicates=True):
        for entry in catalog:
            if skip_duplicates or entry.name not in self.files:
                self.append(entry)

    def __iter__(self):
        self._iter_keys = [x for x in self.files]
        self._iter_keys.sort(key=_catalog_file_sort_index, reverse=True)
        return self

    def __next__(self):
        if self._iter_keys:
            yield self.files[self._iter_keys.pop()]
        raise StopIteration()


    @functools.cached_property
    def enc_root(self):
        return self.path.parent


def _catalog_file_sort_index(filename):
    if filename.endswith("CATALOG.031"):
        return "0_{}".format(filename)
    elif filename[-3:].isdigit():
        return "1_{}_{}".format(filename[-3:], filename)
    else:
        return "2_{}".format(filename)
