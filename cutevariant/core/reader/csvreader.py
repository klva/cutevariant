from .abstractreader import AbstractReader
from ..model import Variant, Field
import csv


def fields_from_header(header:[str]) -> [{str: str}]:
    for cell in header:
        yield {'name': cell, 'value_type': 'String'}

def variant_from_row(row:[str]) -> dict:
    return {
        'chr': row[0],
        'pos': row[1],
        'ref': row[2],
        'alt': row[3],
    }


def reader_csv(lines:[str]):
    """Yield, in this order:

    - iterable of fields type
    - iterable of variants

    """
    reader = csv.reader(iter(lines), delimiter="\t")
    headers = tuple(fields_from_header(next(reader)))
    print('HEADERS:', headers)
    yield headers
    yield map(variant_from_row, reader)


class CsvReader(AbstractReader):
    def __init__(self, device):
        super().__init__(device)
        self.fields, self.variants = reader_csv(device)

    def get_fields(self):
        for field in self.fields:
            print('FIELD:', field)
            yield field
        # yield from self.fields

    def get_variants(self):
        # yield from self.variants
        for variant in self.variants:
            print('VARIANT:', variant)
            yield variant


if __name__ == '__main__':
    print('yello')
