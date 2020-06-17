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
from cutevariant.gui.plugins.clinical_info.sql import (
    create_clinical_info,
    insert_clinical_info,
    get_clinical_info,
)

BUILTIN_SELECTIONS = {
    "de-novo-gene-panel": "in_tightlist='True' AND variant_tag = 'de_novo' AND (gene_in_expert_panel = 'True' OR gene_in_hpo_panel = 'True')",
    "biallelic-gene-panel": "in_tightlist='True' AND tightlist_transcript_tag='biallelic' AND (gene_in_expert_panel = 'True' OR gene_in_hpo_panel = 'True')",
    "de-novo-exome": "in_tightlist='True' AND variant_tag = 'de_novo'",
    "biallelic-exome": "in_tightlist='True' AND tightlist_transcript_tag='biallelic'",
}


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="input file path")
    parser.add_argument("output", help="output file path")
    parser.add_argument(
        "--ped", help="ped file of the family, will be used to rename samples"
    )
    parser.add_argument(
        "--clinical_info", help="Clinical information file to inject in the database"
    )
    parser.add_argument(
        "--disable_selections",
        help="Disable creation of built in selections",
        action="store_true",
    )
    args = parser.parse_args()

    if os.path.exists(args.output):
        logger.critical("Destination file exists !")
        sys.exit(1)

    conn = sql.get_sql_connexion(args.output)
    logger.info("Importing database")
    for _, message in async_import_file(conn, args.input):
        print(message)

    if not args.disable_selections:
        logger.info("Creating built-in selections")
        builder = sql.QueryBuilder(conn)
        for name, query in BUILTIN_SELECTIONS.items():
            query = "SELECT chr,pos,ref,alt FROM variants WHERE " + query
            builder.set_from_vql(query)
            builder.save(name)

    if args.clinical_info:
        create_clinical_info(conn)
        with open(args.clinical_info, "r") as clin_f:
            field_name = None
            field_type = None
            field_value = ""
            for line in clin_f:
                line = line.rstrip()
                # This is a header line
                if line.startswith("["):
                    # Save the previous field
                    if field_name:
                        insert_clinical_info(conn, field_name, field_type, field_value)
                    field_name, field_type = line.strip("[]").split(":")
                    field_value = ""
                # This is a data line
                else:
                    if field_value:
                        field_value += "\n"
                    field_value += line
            if field_name:
                insert_clinical_info(conn, field_name, field_type, field_value)

        # Inject family info in the clinical information pane as well
        if args.ped:
            field_name = "Individus"
            field_type = "longtext"
            sequenced_samples = {s["name"]: s["id"] for s in sql.get_samples(conn)}
            index = None
            with open(args.ped, "r") as ped_f:
                for line in ped_f:
                    if line.startswith("#"):
                        continue
                    cols = line.rstrip().split("\t")
                    if cols[1] not in sequenced_samples:
                        continue
                    if {cols[2], cols[3]} & {"", "0"}:
                        continue
                    index = cols[1]
                    father = cols[2]
                    mother = cols[3]
                    logger.info(
                        f"Index is {cols[1]}, father {cols[2]}, mother {cols[3]}"
                    )
                    break
            if index:
                field_value = f"""{index} => index
{father} => père
{mother} => mère"""
                insert_clinical_info(conn, field_name, field_type, field_value)


if __name__ == "__main__":
    main()
