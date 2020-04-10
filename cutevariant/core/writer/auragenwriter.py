import csv
import datetime
import getpass
import sys

import cutevariant.core.sql as sql
from cutevariant import __version__
from cutevariant.core.writer.abstractwriter import AbstractWriter


class AuragenWriter(AbstractWriter):
    """
        TODO
    """

    def __init__(self, device):
        super().__init__(device)

    def save(self, conn):
        print("##!CUTEVARIANT-EXPORT", file=self.device)
        print("##EXPORT-VERSION=1", file=self.device)
        print(f"##CUTEVARIANT-VERSION={__version__}", file=self.device)
        print(f"##EXPORT-DATE={datetime.datetime.now().isoformat()}", file=self.device)
        print(f"##USER:{getpass.getuser()}", file=self.device)
        columns = list(x["name"] for x in sql.get_field_by_category(conn, "variants"))
        columns += list(
            x["name"] for x in sql.get_field_by_category(conn, "annotations")
        )
        columns_str = columns.copy()
        for sample in sql.get_samples(conn):
            for sample_field in sql.get_field_by_category(conn, "samples"):
                columns.append(("genotype", sample["name"], sample_field["name"]))
                columns_str.append(f"{sample['name']}.{sample_field['name']}")
        print("\t".join(columns_str), file=self.device)
        writer = csv.writer(self.device, delimiter="\t")
        filters = {"field": "favorite", "operator": "=", "value": 1}
        builder = sql.QueryBuilder(conn, columns=columns, filters=filters)
        for row in builder.items(limit=9999999):
            writer.writerow(row[1:])


if __name__ == "__main__":
    conn = sql.get_sql_connexion(sys.argv[1])
    w = AuragenWriter(sys.stdout)
    w.save(conn)
