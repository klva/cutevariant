#!/usr/bin/env python3
"""
    Temporary CLI utiity to import a .aura file into a database
"""
import sys
import argparse
import os
import logging

from cutevariant.core import sql
from cutevariant.core.importer import async_import_file


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="input file path")
    parser.add_argument("output", help="output file path")
    parser.add_argument(
        "--ped", help="ped file of the family, will be used to rename samples"
    )
    args = parser.parse_args()

    if os.path.exists(args.output):
        logger.critical("Destination file exists !")
        sys.exit(1)

    conn = sql.get_sql_connexion(args.output)
    logger.info("Importing database")
    for _, message in async_import_file(conn, args.input):
        print(message)

    if args.ped:
        sequenced_samples = {s["name"]: s["id"] for s in sql.get_samples(conn)}
        logger.info("Renaming samples")
        with open(args.ped, "r") as ped_f:
            for line in ped_f:
                if line.startswith("#"):
                    continue
                cols = line.rstrip().split("\t")
                if cols[1] in sequenced_samples:
                    if {cols[2], cols[3]} & {"", "0"}:
                        continue
                    logger.info(
                        f"Index is {cols[1]}, father {cols[2]}, mother {cols[3]}"
                    )
                    sql.update_sample(
                        conn, {"id": sequenced_samples[cols[1]], "name": "index"}
                    )
                    sql.update_sample(
                        conn, {"id": sequenced_samples[cols[2]], "name": "father"}
                    )
                    sql.update_sample(
                        conn, {"id": sequenced_samples[cols[3]], "name": "mother"}
                    )
                    break


if __name__ == "__main__":
    main()
