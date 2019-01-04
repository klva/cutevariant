import pytest
import sys
import os
import sqlite3
import warnings
from cutevariant.core.importer import import_file
from cutevariant.core.query import QueryBuilder 

@pytest.fixture
def conn():
    db_path = "/tmp/test_cutevaiant.db"
    import_file("exemples/test.csv", db_path)
    conn = sqlite3.connect(db_path)
    return conn


def test_results(conn):

    builder = QueryBuilder(conn)
    real_row_number = sum(1 for line in open("exemples/test.csv"))

    # test query output as row by record 
    assert len(list(builder.rows())) == real_row_number - 1 , "wrong record numbers " 
  # test query output as row by record 
    assert len(list(builder.items())) == real_row_number - 1 , "wrong record numbers " 



    # test where clause
    builder.where = "chr == 'chr5'"
    assert len(list(builder.rows())) == 1 , "wrong record numbers"

    # Test sample jointure 
    print(builder.query())


    conn.close()


def test_detect_samples(conn):
    builder = QueryBuilder(conn)

    # test regular expression in columns 
    builder.columns  = ["chr","pos","ref", "alt", "genotype(\"sacha\").gt"]
    assert "sacha" in builder.detect_samples().keys(), "cannot detect sacha sample in query columns"

    builder.columns  = ["chr","pos","ref", "alt", "genotype(\'sacha\').gt"]
    assert "sacha" in builder.detect_samples().keys(), "cannot detect sacha sample in query columns"

    builder.columns  = ["chr","pos","ref", "alt", "genotype(\'sacha\').gt", "genotype(\"olivier\").gt"]
    assert "sacha" in builder.detect_samples().keys(), "cannot detect sacha "
    assert "olivier" in builder.detect_samples().keys(), "cannot detect olivier "

    # test where clause 
    builder.columns  = ["chr","pos","ref", "alt"]
    builder.where = "genotype(\"sacha\").gt = 1"
    assert "sacha" in builder.detect_samples().keys(), "cannot detect sacha sample in query where clause"

    # test if builder return good samples count 
    builder.columns  = ["chr","pos","ref", "alt", "genotype(\'sacha\').gt", "genotype(\"olivier\").gt"]
    len(set(builder.samples()).intersection(set(["sacha","olivier"]))) == 2  


# def test_sample_query():
#     ''' Test join with samples ''' 
#     db_path = "/tmp/test_cutevaiant.db"
#     import_file("exemples/test.csv", db_path)

#     conn = sqlite3.connect(db_path)

#     builder = QueryBuilder(conn)

#     builder.samples = ["sacha","olivier"]

#     print("test",builder.query())


# def test_limit():












