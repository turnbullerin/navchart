import logging
import iso8211_scratch
import copy
import decimal
import os
import functools
import re
import datetime
from pathlib import Path
from autoinject import injector


@injector.injectable
class S57Standard:

    def __init__(self):
        self._record_name_map = {}
        self._agency_map = {}
        self._object_type_map = {}
        self._attribute_name_map = {}

    def init(self):
        pass

    def attribute_name(self, name_or_num):
        return self._to_str(name_or_num, self._attribute_name_map, "attribute")

    def object_type(self, name_or_num):
        return self._to_str(name_or_num, self._object_type_map, "object type")

    def agency(self, name_or_num):
        return self._to_str(name_or_num, self._agency_map, "agency")

    def record_name(self, name_or_num):
        return self._to_str(name_or_num, self._record_name_map, "record type")

    def _to_str(self, name_or_num, mapping, kind_for_error):
        if isinstance(name_or_num, str) and not name_or_num.isdigit():
            return name_or_num
        self.init()
        num = int(name_or_num)
        if num in mapping:
            return mapping[num]
        logging.getLogger(__name__).warning("Unrecognized {} ID: {}".format(kind_for_error, name_or_num))
        return str(name_or_num)


class S57Cell:

    def __init__(self, cell_file):
        if not cell_file.endswith(".000"):
            raise ValueError("Invalid cell file")
        self.path = Path(cell_file)
        self.multiplication_factors = None
        self.base_loaded_flag = False
        self.updates_loaded_flag = False
        self._features = None
        self._geometries = None
        self.metadata = None
        self.update_file_count = None

    @staticmethod
    def find_all_cells(search_path):
        search_dirs = [search_path]
        while search_dirs:
            dir = search_dirs.pop()
            for file in os.scandir(dir):
                if file.is_dir():
                    search_dirs.append(file.path)
                elif file.name.endswith(".000"):
                    return S57Cell(file.path)

    @functools.cached_property
    def support_files(self):
        file_set = set()
        for feat in self.features():
            if "TXTDSC" in feat:
                file_set.add(feat["TXTDSC"])
            if "NTXTDS" in feat:
                file_set.add(feat["NTXTDS"])
        return file_set

    @functools.cache
    def support_file_real_path(self, filename):
        search_dirs = [self.path.parent]
        while search_dirs:
            dir = search_dirs.pop()
            for file in os.scandir(dir):
                if file.is_dir():
                    search_dirs.append(file.path)
                elif file.name == filename:
                    return file.path
        return None

    @functools.cached_property
    def update_no(self, force_load=False):
        if force_load:
            self._load_updates()
        if self.updates_loaded_flag:
            return self.metadata["DSID"]["UPDN"]
        self._set_update_file_count()
        return self.update_file_count

    @functools.cached_property
    def edition_no(self, force_load=False):
        if not (self.base_loaded_flag or force_load):
            # Fast grab of the edition, which is usually in the top few kB of the file
            with open(self.path, "rb") as h:
                chunk = h.read(1024)
                data = ""
                while chunk:
                    data += chunk.decode('ascii', errors='replace')
                    chunk = h.read(1024)
                    results = re.search(self.path.name + r".(\d+).(\d+).\d{16}", data)
                    if results:
                        return int(results.group(1))
        self._load_base_cell()
        return self.metadata["DSID"]["EDTN"]

    @functools.cached_property
    def issued_date(self):
        self._load_updates()
        return datetime.datetime.strptime(self.metadata["ISDT"], "%Y%m%d")

    @functools.cached_property
    def update_application_date(self):
        self._load_base_cell()
        return datetime.datetime.strptime(self.metadata["UPDT"], "%Y%m%d")

    def features(self, object_types=None):
        self._load_updates()
        if object_types and not isinstance(object_types, list):
            object_types = list(object_types)
        for fid in self._features:
            if object_types is None or self._features[fid].layer in object_types:
                yield self._features[fid]

    def feature(self, long_name):
        self._load_updates()
        return self._features[long_name]

    def geometry(self, name):
        self._load_updates()
        return self._geometries[name]

    def _load_base_cell(self):
        if not self.base_loaded_flag:
            cell_data = S57DataFile(self.path)
            self.multiplication_factors = cell_data.get_multiplication_factors()
            self._features = cell_data.features
            self._geometries = cell_data.geometries
            self.metadata = cell_data.metadata
            self.base_loaded_flag = True

    def _set_update_file_count(self):
        if self.update_file_count is None:
            self.update_file_count = 0
            for i in range(1, 1000):
                path = Path("{}.{:03d}".format(self.path.name[:-3], i))
                if not path.exists():
                    break
                self.update_file_count += 1

    def _load_updates(self):
        if not self.updates_loaded_flag:
            self._load_base_cell()
            self._set_update_file_count()
            for i in range(1, self.update_file_count + 1):
                self._apply_update_file(S57DataFile(
                    Path("{}.{:03d}".format(self.path.name[:-4], i)),
                    *self.multiplication_factors
                ))
            self.updates_loaded_flag = True
            for feature in self._features:
                feature.set_reference_cell(self)
            for geom in self._geometries:
                geom.set_reference_cell(self)

    def _apply_update_file(self, update):
        self._features.update(update.features)
        self._geometries.update(update.geometries)
        for feature_id in update.feature_deletes:
            del self._features[feature_id]
        for geometry_id in update.geometry_deletes:
            del self._geometries[geometry_id]
        self.metadata.update(update.metadata)
        for object_update in update.updates:
            if isinstance(object_update, S57GeometryUpdate):
                object_update.apply(self._geometries[object_update.identifier])
            else:
                object_update.apply(self._features[object_update.identifier])


class S57DataFile:

    standard: S57Standard = None

    @injector.construct
    def __init__(self, path, coordinate_factor=None, sounding_factor=None):
        self._raw = iso8211.ISO8211File()
        self._raw.from_file(path)
        self.coordinate_factor = None
        self.sounding_factor = None
        self.path = path
        self.is_base_cell = str(path).endswith(".000")

        self.geometries = None
        self.features = None
        self.metadata = None
        self.loaded_flag = False
        self.coordinate_factor = coordinate_factor
        self.sounding_factor = sounding_factor
        self.updates = []

    def get_multiplication_factors(self):
        self._build_structure()
        return self.coordinate_factor, self.sounding_factor

    def _build_structure(self):
        if not self.loaded_flag:
            self.geometries = {}
            self.features = {}
            self.metadata = {}
            self.feature_deletes = []
            self.geometry_deletes = []
            for dataset in self._raw.datasets():
                if "VRID" in dataset:
                    if dataset["VRID"][0]["RUIN"] == 2:
                        self.updates.append(
                            S57GeometryUpdate(
                                self.standard,
                                self.coordinate_factor,
                                self.sounding_factor
                            ).from_iso8211(dataset)
                        )
                    elif dataset["VRID"][0]["RUIN"] == 1:
                        geometry = S57Geometry(
                            self.standard,
                            self.coordinate_factor,
                            self.sounding_factor
                        ).from_iso8211(dataset)
                        self.geometries[geometry.identifier] = geometry
                    elif dataset["VRID"][0]["RUIN"] == 3:
                        self.geometry_deletes.append(BaseS57Geometry.geometry_identifier(dataset, self.standard))
                    else:
                        raise ValueError("Unrecognized update instruction {}".format(dataset["VRID"][0]["RUIN"]))
                elif "FOID" in dataset:
                    if dataset["FOID"][0]["RUIN"] == 2:
                        self.updates.append(S57FeatureUpdate(self.standard).from_iso8211(dataset))
                    elif dataset["FOID"][0]["RUIN"] == 1:
                        feature = S57Feature(self.standard).from_iso8211(dataset)
                        self.features[feature.identifier] = feature
                    elif dataset["FOID"][0]["RUIN"] == 3:
                        self.feature_deletes.append(BaseS57Feature.feature_identifier(dataset, self.standard))
                elif "DSID" in dataset or "DSPM" in dataset or "DSSI" in dataset:
                    self._process_metadata_dataset(dataset)
            self.loaded_flag = True

    def _process_metadata_dataset(self, data: iso8211.Record):
        if "DSID" in data:
            self.metadata["DSID"] = data["DSID"][0]
        if "DSPM" in data:
            self.metadata["DSPM"] = data["DSPM"][0]
            if self.coordinate_factor is None and "COMF" in data["DSPM"][0] and data["DSPM"][0]["COMF"]:
                self.coordinate_factor = data["DSPM"][0]["COMF"]
            if self.sounding_factor is None and "SOMF" in data["DSPM"][0] and data["DSPM"][0]["SOMF"]:
                self.sounding_factor = data["DSPM"][0]["SOMF"]
        if "DSSI" in data:
            self.metadata["DSSI"] = data["DSSI"][0]


class S57Object:

    def __init__(self, standard):
        self.identifier = None
        self.metadata = None
        self.spatial_references = []
        self.cell = None
        self.standard = standard

    def set_reference_cell(self, cell: S57Cell):
        self.cell = None

    def _build_spatial_reference(self, record_set):
        sref = {
            "NAME": "{}_{}".format(
                self.standard.record_name(record_set["NAME"][-1]),
                iso8211.bytes_to_int(record_set["NAME"][:-1])
            ),
            "ORNT": record_set["ORNT"],
            "USAG": record_set["USAG"],
            "MASK": record_set["MASK"]
        }
        if "TOPI" in record_set:
            sref["TOPI"] = record_set["TOPI"]
        return sref

    @staticmethod
    def apply_update(mode: int, index: int, length: int, target_list: list, new_data: list = None):
        if mode == 1:
            return target_list[0:index-1] + new_data + target_list[index-1:]
        elif mode == 2:
            return target_list[0:index] + target_list[index+length:]
        elif mode == 3:
            return target_list[0:index] + new_data + target_list[index+length:]
        else:
            raise ValueError("Invalid mode: {}".format(mode))


class BaseS57Feature(S57Object):

    def __init__(self, standard):
        super().__init__(standard)
        self.layer = None
        self.attributes = {}

    @staticmethod
    def feature_identifier(data, standard: S57Standard):
        return "{}_{}_{}".format(
            standard.agency(data["FOID"][0]["AGEN"]),
            data["FOID"][0]["FIDN"],
            data["FOID"][0]["FIDS"],
        )

    def from_iso8211(self, data: iso8211.Record):
        self.identifier = BaseS57Feature.feature_identifier(data, self.standard)
        self.metadata = copy.deepcopy(data["FOID"][0])
        self.metadata.update(data["FRID"][0])
        self.layer = self.standard.object_type(self.metadata["OBJL"])
        if "ATTF" in data:
            for attribute in data["ATTF"]:
                self.attributes[self.standard.attribute_name(attribute["ATTL"])] = attribute["ATVL"]
        if "NATF" in data:
            for attribute in data["NATF"]:
                self.attributes[self.standard.attribute_name(attribute["ATTL"])] = attribute["ATVL"]
        return self

    def _build_feature_reference(self, record_set):
        return {
            "LNAM": "{}_{}_{}".format(
                self.standard.agency(iso8211.bytes_to_int(record_set["LNAM"][6:])),
                iso8211.bytes_to_int(record_set["LNAM"][2:6]),
                iso8211.bytes_to_int(record_set["LNAM"][0:2])
            ),
            "RIND": record_set["RIND"],
            "COMT": record_set["COMT"]
        }


class S57Feature(BaseS57Feature):

    def __init__(self, standard):
        super().__init__(standard)
        self.feature_references = []

    def __contains__(self, item):
        return item in self.attributes

    def __getitem__(self, item):
        return self.attributes[item]

    def from_iso8211(self, data: iso8211.Record):
        super().from_iso8211()
        if "FFPT" in data:
            for feature_pointer in data["FFPT"]:
                self.feature_references.append(self._build_feature_reference(feature_pointer))
        if "FSPT" in data:
            for spatial_pointer in data["FSPT"]:
                self.spatial_references.append(self._build_spatial_reference(spatial_pointer))
        return self

    @functools.cached_property
    def geometry(self):
        if self.metadata["PRIM"] == 255:
            return "NONE", []
        geometry_type = None
        points = []
        inner_cut = [[]]
        for ref in self.spatial_references:
            geom = self.cell.geometry(ref["NAME"])
            geom_points = geom.points()
            # Reverse if required
            if ref["ORNT"] == 2:
                geom_points.reverse()
            # Check if we are dealing with an independent point set
            if geom.record_name in ("VC", "VI"):
                geometry_type = "MULTIPOINT"
            elif geometry_type == "MULTIPOINT":
                raise ValueError("Discovered non-point record after point record")
            # Check if it is an inner edge
            if geom.record_name == "VE" and ref["USAG"] == 2:
                inner_cut[-1].extend(geom_points)
            # Outer edge
            else:
                points.extend(geom_points)
        if len(points) == 0:
            return "NONE", []
        elif self.metadata["PRIM"] == 1:
            return ("POINT", points[0]) if len(points) == 1 else ("MULTIPOINT", points)
        elif self.metadata["PRIM"] == 2:
            return "LINESTRING", points
        elif self.metadata["PRIM"] == 3:
            return "POLYGON", [points, *inner_cut]
        raise ValueError("Unknown PRIM {}".format(self.metadata["PRIM"]))

    @functools.cached_property
    def wkt(self):
        datatype, point_list = self.geometry
        if datatype == "NONE":
            return None
        if datatype == "POINT":
            return "POINT ({} {})".format(*point_list)
        if datatype == "MULTIPOINT":
            return "MULTIPOINT ({})".format(",".join("({} {})".format(*point) for point in point_list))
        if datatype == "LINESTRING":
            return "LINESTRING ({})".format(",".join("{} {}".format(*point) for point in point_list))
        if datatype == "POLYGON":
            return "POLYGON ({})".format(",".join("({})".format(",".join("{} {}".format(*point) for point in section)) for section in point_list))
        raise ValueError("Unrecognized geometry type {}".format(datatype))


class S57FeatureUpdate(BaseS57Feature):

    def __init__(self, standard):
        super().__init__(standard)
        self.spatial_ref_update = None
        self.feature_ref_update = None

    def from_iso8211(self, data: iso8211.Record):
        super().from_iso8211()
        if "FFPC" in data:
            self.feature_ref_update = (data["FFPC"][0], data["FFPT"])
        if "FSPC" in data:
            self.spatial_ref_update = (data["FSPC"][0], data["FSPT"])
        return self

    def apply(self, feature: S57Feature):
        if self.spatial_ref_update:
            feature.spatial_references = S57Object.apply_update(
                self.spatial_ref_update[0]["FSUI"],
                self.spatial_ref_update[0]["FSIX"],
                self.spatial_ref_update[0]["NSPT"],
                feature.spatial_references,
                [self._build_spatial_reference(fspt) for fspt in self.spatial_ref_update[1]]
            )
        if self.feature_ref_update:
            feature.spatial_references = S57Object.apply_update(
                self.feature_ref_update[0]["FFUI"],
                self.feature_ref_update[0]["FFIX"],
                self.feature_ref_update[0]["NFPT"],
                feature.feature_references,
                [self._build_feature_reference(ffpt) for ffpt in self.feature_ref_update[1]]
            )
        if self.attributes:
            for attr_name in self.attributes:
                if self.attributes[attr_name] == '⌂':
                    if attr_name in feature.attributes:
                        del feature.attributes[attr_name]
                else:
                    feature.attributes[attr_name] = attr_name


class BaseS57Geometry(S57Object):

    def __init__(self, standard, comf, somf):
        super().__init__(standard)
        self.coordinate_factor = decimal.Decimal(comf)
        self.sounding_factor = decimal.Decimal(somf)
        self.record_name = None

    @staticmethod
    def geometry_identifier(data, standard: S57Standard):
        return "{}_{}".format(
            standard.record_name(data["VRID"][0]["RCNM"]),
            data["VRID"][0]["RCID"]
        )

    def from_iso8211(self, data: iso8211.Record):
        self.identifier = BaseS57Geometry.geometry_identifier(data, self.standard)
        self.record_name = self.standard.record_name(data["VRID"][0]["RCNM"])
        self.metadata = data["VRID"][0]
        return self

    def _build_geometry(self, points, dimensions):
        real_points = []
        for point in points:
            coordinates = [
                decimal.Decimal(point["XCOO"]) / self.coordinate_factor,
                decimal.Decimal(point["YCOO"]) / self.coordinate_factor
            ]
            if dimensions > 2:
                coordinates.append(decimal.Decimal(point["VE3D"]) / self.sounding_factor)
            real_points.append(coordinates)
        return real_points


class S57Geometry(BaseS57Geometry):

    def __init__(self, standard, comf, somf):
        super().__init__(standard, comf, somf)
        self.geometry = []

    def from_iso8211(self, data: iso8211.Record):
        super().from_iso8211(data)
        if "SG3D" in data:
            self.geometry = self._build_geometry(data["SG3D"], 3)
        elif "SG2D" in data:
            self.geometry = self._build_geometry(data["SG2D"], 2)
        if "VRPT" in data:
            for spatial_pointer in data["VRPT"]:
                self.spatial_references.append(self._build_spatial_reference(spatial_pointer))
        return self

    @functools.cached_property
    def points(self):
        full_point_list = copy.deepcopy(self.geometry)
        if self.record_name == "VI" or self.record_name == "VC":
            if self.spatial_references:
                raise ValueError("Independent/node types shouldn't have spatial references?")
        elif self.record_name == "VE":
            # Check for start and end nodes
            for ref in self.spatial_references:
                referenced_geometry = self.cell.geometry(ref["NAME"])
                if not referenced_geometry.record_name == 'VC':
                    raise ValueError("Edge records should have a record name of VC")
                if referenced_geometry["TOPI"] == 1:
                    full_point_list = referenced_geometry.points() + full_point_list
                elif referenced_geometry["TOPI"] == 2:
                    full_point_list += referenced_geometry.points()
                else:
                    raise ValueError("Unknown value for TOPI: {}".format(referenced_geometry["TOPI"]))
        else:
            raise ValueError("Unknown record name {}".format(self.record_name))
        return full_point_list


class S57GeometryUpdate(S57Geometry):

    def __init__(self, standard, comf, somf):
        super().__init__(standard, comf, somf)
        self.spatial_ref_update = None
        self.geometry_update = None

    def from_iso8211(self, data: iso8211.Record):
        super().from_iso8211(data)
        if "SGCC" in data:
            if "SG3D" in data:
                self.geometry_update = (data["SGCC"], data["SG3D"], 3)
            else:
                self.geometry_update = (data["SGCC"], data["SG2D"], 2)
        if "VRPC" in data:
            self.spatial_ref_update = (data["VRPC"], data["VRPT"])
        return self

    def apply(self, geometry: S57Geometry):
        if self.spatial_ref_update:
            geometry.spatial_references = S57Object.apply_update(
                self.spatial_ref_update[0]["FSUI"],
                self.spatial_ref_update[0]["FSIX"],
                self.spatial_ref_update[0]["NSPT"],
                geometry.spatial_references,
                [self._build_spatial_reference(fspt) for fspt in self.spatial_ref_update[1]]
            )
        if self.geometry_update:
            geometry.geometry = S57Object.apply_update(
                self.geometry_update[0]["CCUI"],
                self.geometry_update[0]["CCIX"],
                self.geometry_update[0]["CCNC"],
                geometry.geometry,
                self._build_geometry(self.geometry_update[1], self.geometry_update[2])
            )
