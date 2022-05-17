import decimal
import logging
import enum


def bytes_to_int(byte_list):
    num = 0
    for byte in byte_list:
        num = (num << 8) | byte
    return num


class ArrayDescriptor:

    def __init__(self):
        self.array_values = {}
        self.array_order = []

    def raw(self, descriptor, format_code):
        self.array_order.append(descriptor)
        self.array_values[descriptor] = format_code
        return self

    def string(self, descriptor, length=None):
        if length is None:
            return self.raw(descriptor, 'A')
        else:
            return self.raw(descriptor, 'A({})'.format(length))

    def integer(self, descriptor, byte_size=None):
        if byte_size is None:
            return self.raw(descriptor, 'I')
        else:
            return self.raw(descriptor, 'I({})'.format(byte_size))

    def real(self, descriptor, byte_size=None):
        if byte_size is None:
            return self.raw(descriptor, 'R')
        else:
            return self.raw(descriptor, 'R({})'.format(byte_size))

    def binary_integer(self, descriptor, byte_size, signed=False):
        return self.raw(descriptor, 'b{}{}'.format(1 if not signed else 2, byte_size))

    def raw_binary(self, descriptor, byte_size):
        return self.raw(descriptor, 'B({})'.format(byte_size * 8))

    def __iter__(self):
        return iter(self.array_order)

    def __getitem__(self, item):
        return self.array_values[item]


class DataStream:

    TO_FT = -1
    TO_UT = -2
    TO_FTUT = -4

    FT = 30
    UT = 31

    FT_CHAR = "\x1e"
    UT_CHAR = "\x1f"

    def __init__(self, data=[]):
        self.data = data
        self.index = 0

    def empty(self):
        return self.index >= len(self.data)

    def peek(self):
        return self.data[self.index]

    def seek(self, byte, start_at=0):
        for i in range(start_at, len(self.data)):
            if self.data[i] == byte:
                return i
        return None

    def read(self, length):
        chunk = None
        if length == DataStream.TO_FT:
            pos = self.seek(DataStream.FT, self.index)
            if pos is not None:
                chunk = self.data[self.index:pos]
                self.index = pos + 1
        elif length == DataStream.TO_UT:
            pos = self.seek(DataStream.UT, self.index)
            if pos is not None:
                chunk = self.data[self.index:pos]
                self.index = pos + 1
        else:
            chunk = self.data[self.index:self.index+length]
            self.index += length
        return chunk

    def write(self, byte_list):
        self.data.append(byte_list)
        return len(byte_list)

    def read_str(self, length, encoding='ascii'):
        chunk = bytes(self.read(length))
        return chunk.decode(encoding) if chunk else None

    def write_str(self, string, encoding='ascii'):
        return self.write(string.encode(encoding))

    def read_int(self, length):
        chunk = self.read_str(length)
        return int(chunk) if chunk else None

    def write_int(self, number, length=None):
        return self.write_str(self._fix_number(number, length))

    def read_decimal(self, length):
        print(length)
        chunk = self.read_str(length)
        print(chunk)
        return decimal.Decimal(chunk) if chunk else None

    def write_decimal(self, number, length=None):
        return self.write_str(self._fix_number(number, length))

    def _fix_number(self, number, length=None):
        if length is None:
            return str(number)
        num = str(number)
        while len(num) < length:
            num = "0" + num
        return num

    def read_binary_int(self, length, signed=False):
        byte_list = self.read(length)
        bit_count = 8 * len(byte_list)
        num = 0
        for v in reversed(byte_list):
            num = (num << 8) | v
        if signed and len(bin(num)[2:]) == bit_count:
            # Leading one means we have a negative number, convert according to ISO-8211 standard
            num = int(bin(num)[3:], 2) + (-1 * (2 ** (bit_count - 1)))
        return num

    def write_binary_int(self, number, length):
        if number >= 0:
            return self.write(number.to_bytes(length, byteorder='big'))
        else:
            data = number.to_bytes(length, byteorder='little')
            data[0] += 128
            return self.write(bytes(reversed(data)))

    def read_bytes(self, length, le_transform=False):
        data = self.read(length)
        if le_transform:
            return bytes(reversed(data))
        else:
            return bytes(data)

    def write_bytes(self, data, be_transform=False):
        if be_transform:
            return self.write(bytes(reversed(data)))
        else:
            return self.write(bytes(data))


class FieldDescriptor:

    def __init__(self, tag_name, parent_tag=None):
        self.tag_name = tag_name
        self.structure = 0
        self.data_type = 0
        self.auxiliary = '00'
        self.graphics = ';&'
        self.escape = '   '
        self.parent_tag = parent_tag

    def length(self):
        return 11

    def to_iso8211(self):
        data = [
            str(self.structure),
            str(self.data_type),
            self.auxiliary,
            self.graphics,
            self.escape,
            DataStream.UT_CHAR
        ]
        return ''.join(data)

    @staticmethod
    def from_stream_base(field, stream):
        field.structure = stream.read_int(1)
        field.data_type = stream.read_int(1)
        field.auxiliary = stream.read_str(2)
        field.graphics = stream.read_str(2)
        field.escape = stream.read_str(3)
        return field


class DataFieldDescriptor(FieldDescriptor):

    def __init__(self, tag_name, long_name, parent_tag=None):
        super().__init__(tag_name, parent_tag)
        self.long_name = long_name

    def length(self):
        return super().length() + len(self.long_name) + 1

    def to_iso8211(self):
        return super().to_iso8211() + self.long_name + DataStream.UT_CHAR

    def data_from_stream(self, stream):
        pass

    @staticmethod
    def value_from_stream(format_code, stream):
        read_length = DataFieldDescriptor._interpret_field_length(format_code)
        print(format_code, read_length)
        if format_code[0] == 'A':
            # TODO: Check how we pull the encoding
            return stream.read_str(read_length, 'latin-1')
        if format_code[0] == "I":
            return stream.read_int(read_length)
        if format_code[0] == "R":
            return stream.read_decimal(read_length)
        if format_code[0] == "b":
            return stream.read_binary_int(read_length, signed=format_code[1] == "2")
        if format_code[0] == "B":
            return stream.read_bytes(read_length, le_transform=True)
        raise ValueError("Unsupported field format for parsing: {}".format(format_code))

    @staticmethod
    def _interpret_field_length(format_code):
        if len(format_code) == 1:
            return DataStream.TO_FTUT
        elif format_code[1] == "(":
            num = int(format_code[2:-1])
            return int(num / 8) if format_code[0] == "B" else num
        elif format_code[0] == "b":
            return int(format_code[2:])
        else:
            raise ValueError("Unsupported field format for length: {}".format(format_code))

    @staticmethod
    def from_stream(tag_name, stream, control_field):
        header_info = DataStream(stream.read(9))
        long_name = stream.read_str(DataStream.TO_UT)
        descriptors = stream.read_str(DataStream.TO_UT)
        formats = stream.read_str(DataStream.TO_FT)[1:-1]
        if not descriptors:
            f = SingleValueDataFieldDescriptor.from_stream_components(tag_name, long_name, formats)
        else:
            f = ArrayDataFieldDescriptor.from_stream_components(tag_name, long_name, descriptors, formats)
        FieldDescriptor.from_stream_base(f, header_info)
        for parent in control_field.data_tree:
            if f.tag_name in control_field.data_tree[parent]:
                f.parent_tag = parent
        return f


class SingleValueDataFieldDescriptor(DataFieldDescriptor):

    def __init__(self, long_name, tag_name, frmt, parent_tag=None):
        super().__init__(tag_name, long_name, parent_tag)
        self.format_code = frmt
        self.structure = 0
        self.data_type = 1 if not frmt.startswith("b") else 2

    def length(self):
        return super().length() + 2 + len(self.format_code)

    def to_iso8211(self):
        return ''.join([
            super().to_iso8211(),
            DataStream.UT_CHAR,
            self.format_code,
            DataStream.FT_CHAR
        ])

    def data_from_stream(self, stream):
        return DataFieldDescriptor.value_from_stream(self.format_code, stream)

    @staticmethod
    def from_stream_components(tag_name, long_name, formats):
        return SingleValueDataFieldDescriptor(long_name, tag_name, formats)


class ArrayDataFieldDescriptor(DataFieldDescriptor):

    def __init__(self, long_name, tag_name, multi_valued=False, parent_tag=None):
        super().__init__(tag_name, long_name, parent_tag)
        self.internal_structure = {}
        self.structure_order = []
        self.structure = 1 if not multi_valued else 2
        self.data_type = 6

    def add_sub_field(self, tag_name, field_format):
        self.structure_order.append(tag_name)
        self.internal_structure[tag_name] = field_format

    def length(self):
        total = super().length()
        # Descriptor fields
        total += len(self._descriptor_list())
        total += 1
        # Format fields
        total += len(self._format_list())
        total += 1
        return total

    def data_from_stream(self, stream):
        print(self.internal_structure)
        values = []
        while not stream.empty():
            arr = {}
            for idx, key in enumerate([x for x in self.structure_order]):
                arr[key] = DataFieldDescriptor.value_from_stream(self.internal_structure[key], stream)
            values.append(arr)
        if self.structure == 2:
            return values
        else:
            return values[0]

    def _descriptor_list(self):
        return '!'.join([x for x in self.structure_order])

    def _format_list(self):
        format_codes = []
        count = 0
        buffered = None
        for desc in self.structure_order:
            code = self.internal_structure[desc]
            if buffered is None:
                buffered = code
                count = 1
                continue
            elif buffered == code:
                count += 1
            else:
                format_codes.append(buffered if count == 1 else "{}{}".format(count, buffered))
                buffered = code
                count = 1
        format_codes.append(buffered if count == 1 else "{}{}".format(count, buffered))
        return ','.join(format_codes)

    def to_iso8211(self):
        data = [
            super().to_iso8211(),
            '*' if self.structure == 2 else '',
            self._descriptor_list(),
            DataStream.UT_CHAR,
            self._format_list(),
            DataStream.FT_CHAR
        ]
        return ''.join(data)

    @staticmethod
    def from_stream_components(tag_name, long_name, descriptors, formats):
        f = ArrayDataFieldDescriptor(long_name, tag_name)
        format_codes = []
        for code in formats.split(","):
            count_str = ""
            while code[0].isdigit():
                count_str += code[0]
                code = code[1:]
            if not count_str:
                format_codes.append(code)
            else:
                format_codes.extend([code for x in range(0, int(count_str))])
        descriptor_list = descriptors.split("!")
        for i in range(0, len(descriptor_list)):
            f.add_sub_field(descriptor_list[i], format_codes[i])
        return f


class ControlFieldDescriptor(FieldDescriptor):

    def __init__(self, tag_length):
        super().__init__('0' * tag_length)
        self.data_tree = {}
        self.root_node = None

    def set_data_tree(self, new_tree, root_node):
        self.data_tree = new_tree
        self.root_node = root_node

    def length(self):
        return super().length() + (2 * len(self.tag_name) * len(self.data_tree))

    def _tree_entries(self, parent=None):
        if parent is None:
            parent = self.root_node
        for child_key in self.data_tree[parent]:
            yield parent, child_key
            if child_key in self.data_tree:
                for p, c in self._tree_entries(child_key):
                    yield p, c

    def to_iso8211(self):
        data = [super().to_iso8211()]
        for parent, child in self._tree_entries():
            data.append("{}{}".format(parent, child))
        data.append(DataStream.FT_CHAR)
        return ''.join(data)

    @staticmethod
    def from_stream(stream, tag_size):
        field = ControlFieldDescriptor(tag_size)
        FieldDescriptor.from_stream_base(field, stream)
        stream.read(DataStream.TO_UT)
        tags = stream.read_str(DataStream.TO_FT)
        pair_size = tag_size * 2
        num_pairs = int(len(tags) / pair_size)
        for i in range(0, num_pairs):
            start = pair_size * i
            middle = start + tag_size
            end = middle + tag_size
            parent = tags[start:middle]
            child = tags[middle:end]
            if field.root_node is None:
                field.root_node = parent
            if parent not in field.data_tree:
                field.data_tree[parent] = []
            field.data_tree[parent].append(child)
        return field


class Header:

    def __init__(self, tag_size=4):
        self.tag_size = tag_size
        self.interchange = '3'
        self.leader = 'L'
        self.extension = 'E'
        self.version = '1'
        self.app = ' '
        self.fc_length = 9
        self.base = 0
        self.charset = ' ! '
        self.length_size = 0
        self.position_size = 0
        self.future_flag = 0
        self.length = 0
        self._field_list = []

    @staticmethod
    def from_stream_base(obj, stream: DataStream):
        obj.length = stream.read_int(5)
        obj.interchange = stream.read_str(1)
        obj.leader = stream.read_str(1)
        obj.extension = stream.read_str(1)
        obj.version = stream.read_str(1)
        obj.app = stream.read_str(1)
        obj.fc_length = stream.read_str(2)
        obj.base = stream.read_int(5)
        obj.charset = stream.read_str(3)
        obj.length_size = stream.read_int(1)
        obj.position_size = stream.read_int(1)
        obj.future_flag = stream.read_int(1)
        obj.tag_size = stream.read_int(1)

    @staticmethod
    def read_data_directory(metadata, stream: DataStream):
        field_list = {}
        d_stream = DataStream(stream.read(DataStream.TO_FT))
        while not d_stream.empty():
            tag = d_stream.read_str(metadata.tag_size)
            leng = d_stream.read_int(metadata.length_size)
            pos = d_stream.read_int(metadata.position_size)
            field_list[tag] = [leng, pos]
        metadata._field_list = field_list
        return field_list


class Record(Header):

    def __init__(self, file_metadata, tag_size=4):
        super().__init__(tag_size)
        self.file_metadata = file_metadata
        self._fields = {}

    def __getitem__(self, item):
        return self._fields[item]

    def __contains__(self, item):
        return item in self._fields

    def __iter__(self):
        return iter(self._fields)

    def __setitem__(self, item, value):
        if not isinstance(value, Field):
            self._fields[item] = Field(self.file_metadata, item).from_value(value)
        else:
            self._fields[item] = value

    @staticmethod
    def from_stream(file_metadata, stream):
        metadata = Record(file_metadata)
        Header.from_stream_base(metadata, stream)
        field_list = Header.read_data_directory(metadata, stream)
        for tag in field_list:
            if all(x == "0" for x in tag):
                continue
            metadata[tag] = Field(file_metadata, tag).from_stream(DataStream(stream.read(field_list[tag][0])))
        return metadata


class Field:

    def __init__(self, file_metadata, tag_name):
        self.tag_name = tag_name
        self.field_info = file_metadata.fields[tag_name]
        self.data = None
        self.data_type = None

    def from_value(self, d):
        self.data = d
        return self

    def from_stream(self, stream: DataStream):
        self.data = self.field_info.data_from_stream(stream)
        return self


class Metadata(Header):

    def __init__(self, root_element=None, tag_size=4):
        super().__init__(tag_size)
        self.fields = {}
        if root_element is None:
            frmt = "{:0" + str(tag_size) + "d}"
            self.root_element = frmt.format(1)
        else:
            self.root_element = root_element
        self.control_field = None
        self.field_order = []

    def to_iso8211(self):
        self._set_sizes()
        data = [
            "{:05d}".format(self.length),
            self.interchange,
            self.leader,
            self.extension,
            self.version,
            self.app,
            "{:02d}".format(self.fc_length),
            "{:05d}".format(self.base),
            self.charset,
            str(self.length_size),
            str(self.position_size),
            str(self.future_flag),
            str(self.tag_size)
        ]
        position = 0
        pos_format = "{:0" + str(self.position_size) + "d}"
        len_format = "{:0" + str(self.length_size) + "d}"
        for f_name in self.field_order:
            field = self.fields[f_name]
            data.append(field.tag_name)
            leng = field.length()
            data.append(len_format.format(leng))
            data.append(pos_format.format(position))
            position += leng
        data.append(DataStream.FT_CHAR)
        for f_name in self.field_order:
            data.append(self.fields[f_name].to_iso8211())
        return ''.join(data)

    def _set_sizes(self):
        self.length = 0
        max_position = 0
        max_length = 0
        previous_length = 0
        map = {}
        for f_name in self.fields:
            field = self.fields[f_name]
            if field.parent_tag:
                if field.parent_tag not in map:
                    map[field.parent_tag] = []
                map[field.parent_tag].append(field.tag_name)
            max_position += previous_length
            leng = field.length()
            self.length += leng
            if leng > max_length:
                max_length = leng
            previous_length = leng
        self.length_size = len(str(max_length))
        self.position_size = len(str(max_position))
        self.base = ((self.length_size + self.position_size + self.tag_size) * len(self.fields)) + 24
        self.length += self.base
        self.control_field.set_data_tree(map, self.root_element)

    def add_field(self, field):
        if not len(field.tag_name) == self.tag_size:
            raise ValueError("Invalid tag size: expecting {} found {}".format(self.tag_size, len(field.tag_name)))
        if field.tag_name in self.fields:
            logging.getLogger(__name__).warning("Overwriting field {}, already exists".format(field.tag_name))
        if isinstance(field, ControlFieldDescriptor):
            self.control_field = field
        self.fields[field.tag_name] = field
        self.field_order.append(field.tag_name)

    def add_control(self):
        self.add_field(ControlFieldDescriptor(self.tag_size))

    def add_basic_field(self, long_name: str, tag_name: str, frmt: str, parent_tag: str = None):
        self.add_field(SingleValueDataFieldDescriptor(long_name, tag_name, frmt, parent_tag))

    def add_array_field(self, long_name: str, tag_name: str, desc_format: dict, allow_multiples: bool = False, parent_tag: str = None):
        f = ArrayDataFieldDescriptor(long_name, tag_name, allow_multiples, parent_tag)
        for descriptor in desc_format:
            f.add_sub_field(descriptor, desc_format[descriptor])
        self.add_field(f)

    @staticmethod
    def from_stream(stream: DataStream):
        metadata = Metadata()
        Header.from_stream_base(metadata, stream)
        field_list = Header.read_data_directory(metadata, stream)
        control_field = None
        for tag_name in field_list:
            if tag_name == ("0" * metadata.tag_size):
                control_field = ControlFieldDescriptor.from_stream(stream, metadata.tag_size)
                metadata.add_field(control_field)
            else:
                data_field = DataFieldDescriptor.from_stream(tag_name, stream, control_field)
                metadata.add_field(data_field)
        return metadata


class DataFile:

    def __init__(self, metadata=None):
        self.metadata = metadata
        self._records = []

    def add_record(self, r: Record):
        self._records.append(r)

    def __iter__(self):
        return iter(self._records)

    def __getitem__(self, item):
        return self._records[item]

    @staticmethod
    def from_file(file):
        f = DataFile()
        with open(file, "rb") as h:
            stream = DataStream(list(h.read()))
            f.metadata = Metadata.from_stream(stream)
            while not stream.empty():
                f.add_record(Record.from_stream(f.metadata, stream))
        return f
