import csv
import os
from collections import defaultdict

from .abstractreader import AbstractReader
from cutevariant.commons import logger, DIR_DATA

LOGGER = logger

# This is still very much unfinished and potentially buggy
# Some of the big things :
# TODO Documentation
# TODO Test
# TODO proper integration into the readerfactory and the wizard
# TODO speed


class RvcfReader(AbstractReader):
    """Parser to read data from CSV files produced by Rvcf and the AURAGEN
    curation pipeline
    TODO more thorough explanations of the format
    """

    def __init__(self, device):
        super().__init__(device)

        # These columns are processed manually
        self.ignored_columns = ("CHROM", "POS", "REF", "ALT")
        # Stores normalized field data
        self.field_info_cache = {}

        # TODO do we need to sniff the dialect ? our file seem to use the
        # standard excel format (header on first line without a #, comma
        # separator, quoted only when necessary)
        self.csv_reader = csv.DictReader(device)
        self.parse_samples()

        # We store the definitions for all fields in a csv file, since they
        # can't be built in the annotation file unlike VCF
        with open(
            os.path.join(DIR_DATA, "auragen_field_descriptions.csv"), "r"
        ) as desc_f:
            reader = csv.DictReader(desc_f)
            self.descriptions = {r["field"]: r["description"] for r in reader}

    def get_fields(self):
        # TODO Use VEP AnnotationParser ?
        # Fixed columns
        yield {
            "name": "chr",
            "category": "variants",
            "description": "chromosome",
            "type": "str",
            "constraint": "NOT NULL",
        }
        yield {
            "name": "pos",
            "category": "variants",
            "description": "position",
            "type": "int",
            "constraint": "NOT NULL",
        }
        yield {
            "name": "ref",
            "category": "variants",
            "description": "reference base(s)",
            "type": "str",
            "constraint": "NOT NULL",
        }
        yield {
            "name": "alt",
            "category": "variants",
            "description": "alternative base(s)",
            "type": "str",
            "constraint": "NOT NULL",
        }

        seen_sample_columns = set()
        for field in self.csv_reader.fieldnames:
            if field in self.ignored_columns:
                continue

            name, category, field_type = self.classify_field(field)
            if category == "samples":
                if name in seen_sample_columns:
                    continue
                seen_sample_columns.add(name)

            yield {
                "name": name,
                "category": category,
                "description": self.descriptions.get(name, ""),
                "type": field_type,
            }

        # This is an additionnal field we generate containing the original value of gt
        yield {
            "name": "raw_gt",
            "category": "samples",
            "description": self.descriptions.get("raw_gt", ""),
            "type": "str",
        }

    def convert_gt(self, gt):
        if "/" in gt:
            gt1, gt2 = gt.split("/")
        else:
            gt1, gt2 = gt.split("|")
        if gt1 == ".":
            return -1
        elif gt1 == "0" and gt2 == "0":
            return 0
        elif gt1 == gt2:
            return 2
        else:
            return 1

    def classify_field(self, field):
        """
            Determine the category and the type, and normalize the name of a
            given field
            Returns a 3-uple (name, category, type)

        """
        # TODO when we add descriptions it should be handled here and we should
        # return ready-to-yield dicts instead of tuples
        if field in self.field_info_cache:
            return self.field_info_cache[field]

        field_type = "str"
        # XXX TEMPORARY HACK
        # The fields should be renamed INFO.AURAGEN.transcript_location and
        # INFO.AURAGEN.protein_location
        if field in ("transcript_location", "protein_location"):
            name = field
            category = "annotations"
        elif field.startswith("INFO.CSQ"):
            name = field.split(".", maxsplit=2)[2]
            category = "annotations"
        elif field.startswith("INFO.AURAGEN"):
            name = field.split(".", maxsplit=2)[2]
            category = "annotations"
        elif field.startswith("INFO."):
            name = field.split(".", maxsplit=1)[1]
            category = "variants"
        elif field.rsplit(".", maxsplit=1)[0] in self.samples:
            name = field.rsplit(".", maxsplit=1)[1]
            category = "samples"
            # TODO handle in a more generic way
            if name in ("GT", "DP"):
                field_type = "int"
        else:
            name = field
            category = "variants"

        name = name.lower()
        # TODO this should be elsewhere to cover all readers
        name = name.replace("+", "")
        name = name.replace(".", "")
        if name == "id":
            name = "id2"

        # Rename the "feature" column to transcript beacause CuteVariant
        # expects this column to exist in some places
        if name == "feature":
            name = "transcript"

        self.field_info_cache[field] = (name.lower(), category, field_type)
        return (name.lower(), category, field_type)

    def output_variants(self, line_dict):
        # First, collect all of the "variants" fields from any line, they are
        # fixed
        line = next(iter(line_dict.values()))[0]
        fixed = {"chr": line["CHROM"], "pos": int(line["POS"]), "ref": line["REF"]}
        for field, value in line.items():
            if field in self.ignored_columns:
                continue
            name, category, _ = self.classify_field(field)
            if category == "variants":
                fixed[name] = value

        for allele, lines in line_dict.items():
            # collect all of the "samples" values, they will be the same
            samples_dict = defaultdict(dict)
            for field, value in lines[0].items():
                name, category, field_type = self.classify_field(field)
                if category != "samples":
                    continue
                sample_name = field.rsplit(".", maxsplit=1)[0]
                if name == "gt":
                    samples_dict[sample_name]["raw_gt"] = value
                    value = self.convert_gt(value)
                elif field_type == "int":
                    # TODO handle this better
                    try:
                        value = int(value)
                    except ValueError:
                        continue
                samples_dict[sample_name][name] = value
            samples = []
            for name, dic in samples_dict.items():
                dic["name"] = name
                samples.append(dic)
            annotations = []
            for line in lines:
                annotation = {}
                for field, value in line.items():
                    name, category, field_type = self.classify_field(field)
                    if category != "annotations":
                        continue
                    annotation[name] = value
                annotations.append(annotation)

            yield {
                **fixed,
                "alt": allele,
                "samples": samples,
                "annotations": annotations,
            }

    def get_variants(self):
        # We need to parse each RECNUM as one variant because rvcf ouputs one
        # line per allele and per transcript (i.e a variant with 2 alleles on 2
        # different transcripts will have 4 lines), whereas CuteVariant expects
        # one line per allele, for all variants
        # NOTE This relies on the fact that the recnums in the file are in
        # order
        cur_recnum = "-1"
        lines = defaultdict(list)  # Lines for current RECNUM, grouped by alt alleles
        for line in self.csv_reader:
            if line["RECNUM"] != cur_recnum:
                if lines:
                    yield from self.output_variants(lines)
                lines = defaultdict(list)
                cur_recnum = line["RECNUM"]

            # NOTE Although the name starts with INFO.CSQ, the Allele_id field
            # is computed by the AURAGEN curation pipeline
            alts = line["ALT"].split(",")
            alt = alts[int(line["INFO.CSQ.Allele_index"]) - 1]
            lines[alt].append(line)

    def get_samples(self):
        """We find samples IDs by finding column names not from INFO ending in .GT
        """
        if not self.samples:
            self.parse_samples()
        return self.samples

    def parse_samples(self):
        """We find samples IDs by finding column names not from INFO ending in .GT
        """
        self.samples = []
        for field in self.csv_reader.fieldnames:
            if not field.startswith("INFO") and field.endswith(".GT"):
                self.samples.append(field[:-3])
