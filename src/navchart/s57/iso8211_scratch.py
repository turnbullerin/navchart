

class ISO8211File:

    def __init__(self):
        self.header = None
        self._data = []

    def from_file(self, file):
        with open(file, "rb") as h:
            stream = DataStream(list(h.read()))
            self.header = Leader().from_stream(stream)
            while not stream.empty():
                self._data.append(Record(self.header).from_stream(stream))

    def datasets(self):
        for dataset in self._data:
            yield dataset


class Record(GeneralHeader):

    def __init__(self, header: Leader):
        super().__init__()
        self.header = header
        self._fields = {}

    def from_stream(self, stream: DataStream):
        super().from_stream(stream)
        for d_tag in self.directories:
            d = self.directories[d_tag]
            if d.is_control():
                continue
            self._fields[d_tag] = Field(self.header.get_field_info(d_tag)).from_stream(d.length, stream)

    def __contains__(self, item):
        return item in self._fields[item]

    def __getitem__(self, item):
        return self._fields[item]

    def __iter__(self):
        return iter(self._loaded_order)

    def field_data_from_stream(self, stream: DataStream):
        for i in range(0, len(self.formats)):
            descriptor = self.descriptors[i] or "ITEM{}".format(i)
            # Multi-value fields come with this to indicate it, but we don't need it
            if descriptor.startswith("*"):
                descriptor = descriptor[1:]
            yield descriptor, DataFieldDescriptor._read_field_value(self.formats[i], stream)

    @staticmethod
    def _read_field_value(format_code, stream):
        read_length = DataFieldDescriptor._interpret_field_length(format_code)
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


class Field:

    def __init__(self, field_info: DataFieldDescriptor):
        self.field_info = field_info
        self._data = {}

    def from_stream(self, length: int, stream: DataStream):
        content = DataStream(stream.read(length))
        self._data = {}
        while not content.empty():
            for descriptor, value in self.field_info.field_data_from_stream(content):
                if descriptor not in self._data:
                    self._data[descriptor] = []
                self._data[descriptor].append(value)
        return self

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, item):
        return self._data[item]
