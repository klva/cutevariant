import pytest

from cutevariant.core import sql
from cutevariant.core.reader.bedreader import BedTool


FIELDS = [
    {
        "name": "chr",
        "category": "variants",
        "type": "text",
        "description": "chromosome",
    },
    {"name": "pos", "category": "variants", "type": "int", "description": "position"},
    {"name": "ref", "category": "variants", "type": "text", "description": "reference"},
    {
        "name": "alt",
        "category": "variants",
        "type": "text",
        "description": "alternative",
    },
    {
        "name": "extra1",
        "category": "variants",
        "type": "float",
        "description": "annotation 1",
    },
    {
        "name": "extra2",
        "category": "variants",
        "type": "int",
        "description": "annotation 2",
    },
    {
        "name": "gene",
        "category": "annotations",
        "type": "str",
        "description": "gene name",
    },
    {
        "name": "transcript",
        "category": "annotations",
        "type": "str",
        "description": "transcript name",
    },
    {
        "name": "gt",
        "category": "samples",
        "type": "int",
        "description": "sample genotype",
    },
    {"name": "dp", "category": "samples", "type": "int", "description": "sample dp"},
]

SAMPLES = ["sacha", "boby"]

VARIANTS = [
    {
        "chr": "chr1",
        "pos": 10,
        "ref": "G",
        "alt": "A",
        "extra1": 10,
        "extra2": 100,
        "annotations": [
            {"gene": "gene1", "transcript": "transcript1"},
            {"gene": "gene1", "transcript": "transcript2"},
        ],
        "samples": [
            {"name": "sacha", "gt": 1, "dp": 70},
            {"name": "boby", "gt": 1, "dp": 10},
        ],
    },
    {
        "chr": "chr1",
        "pos": 45,
        "ref": "G",
        "alt": "A",
        "extra1": 20,
        "extra2": 100,
        "annotations": [{"gene": "gene2", "transcript": "transcript2"}],
        "samples": [
            {"name": "sacha", "gt": 0, "dp": 30},
            {"name": "boby", "gt": 0, "dp": 70},
        ],
    },
]


FILTERS = {
    "AND": [
        {"field": "chr", "operator": "=", "value": "chr1"},
        {
            "OR": [
                {"field": "gene", "operator": "=", "value": "gene1"},
                {"field": "pos", "operator": "=", "value": 10},
            ]
        },
    ]
}


## ===========================================================
##
##      Test Misc Function 
##
## ===========================================================

def test_get_sql_connexion():
    assert sql.get_sql_connexion(":memory:") is not None

def test_drop_table():
    conn = sql.get_sql_connexion(":memory:")
    # Create temp table 
    conn.execute("CREATE TABLE test ( name TEXT )")
    # Test if "test" is present
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    "test" in [table[0] for table in tables]
    sql.drop_table(conn, "test")
    # Test if "test" is not present
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    "test" not in [table[0] for table in tables]

def test_clear_table():
    conn = sql.get_sql_connexion(":memory:")
    # Create temp table 
    conn.execute("CREATE TABLE test (name TEXT )")
    conn.execute("INSERT INTO test (name) VALUES ('boby')")
    conn.commit()
    # test count 
    val = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
    assert val == 1 

    #test count after clearing the table 
    sql.clear_table(conn,"test")
    val = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
    assert val == 0 

def test_table_exists():
    conn = sql.get_sql_connexion(":memory:")
    # Create temp table 
    conn.execute("CREATE TABLE test ( name TEXT )")
    assert sql.table_exists(conn, "test") == True

def test_table_count():
    conn = sql.get_sql_connexion(":memory:")
    # Create temp table 
    conn.execute("CREATE TABLE test (name TEXT )")
    conn.execute("INSERT INTO test (name) VALUES ('boby')")
    assert sql.table_count(conn, "test") == 1

def test_get_columns():
    conn = sql.get_sql_connexion(":memory:")
    conn.execute("CREATE TABLE test (name TEXT, age INT )")
    assert sql.get_columns(conn, "test") == ["name","age"]
    

def test_crud_operation():
    # Creata DB
    conn = sql.get_sql_connexion(":memory:")
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INT WITHOUT ROWID )")
    expected_records = [{"name":"sacha", "age":12},{"name":"boby", "age":14}]
    
    # INSERTION 
    sql._create(conn, "test", expected_records[0])
    sql._create(conn, "test", expected_records[1])

    # GET ONE 
    record = sql._get(conn,"test",1) 
    del record["id"]
    assert record == expected_records[0]
    
    # GET LIST 
    assert len(list(sql._get_list(conn,"test"))) == 2
    for i, record in enumerate(sql._get_list(conn,"test")):
        del record["id"]
        assert record == expected_records[i]

    # EDIT 
    update_record = {"name":"olivier", "age":40}
    sql._update(conn, "test", update_record, 1)
    assert sql._get(conn,"test",1)["name"] == "olivier"
    assert sql._get(conn,"test",1)["age"] == 40

    # DELETE Record 
    sql._delete(conn, "test", 1)
    assert len(list(sql._get_list(conn,"test"))) == 1

## ===========================================================
##
##      Create connection for all following test 
##
## ===========================================================

@pytest.fixture
def conn():
    conn = sql.get_sql_connexion(":memory:")

    sql.create_table_project(conn, "test", "hg19")
    assert sql.table_exists(conn, "projects"), "cannot create table fields"

    sql.create_table_fields(conn)
    assert sql.table_exists(conn, "fields"), "cannot create table fields"

    sql.create_many_fields(conn, FIELDS)
    assert sql.table_count(conn, "fields") == len(FIELDS), "cannot insert many fields"

    sql.create_table_selections(conn)
    assert sql.table_exists(conn, "selections"), "cannot create table selections"

    sql.create_table_annotations(conn, sql.get_field_by_category(conn, "annotations"))
    assert sql.table_exists(conn, "annotations"), "cannot create table annotations"

    sql.create_table_samples(conn, sql.get_field_by_category(conn, "samples"))
    assert sql.table_exists(conn, "samples"), "cannot create table samples"
    sql.create_many_samples(conn, SAMPLES)

    sql.create_table_variants(conn, sql.get_field_by_category(conn, "variants"))
    assert sql.table_exists(conn, "variants"), "cannot create table variants"
    sql.create_many_variants(conn, VARIANTS)

    assert conn != None

    return conn

## ===========================================================
##
##      TEST PROJECT
##
## ===========================================================

def test_project(conn):
    # GET 
    project = sql.get_project(conn)
    assert project["name"] == "test"

    #UPDATE 
    sql.update_project(conn,{"name":"new name"})
    assert sql.get_project(conn)["name"] == "new name"

## ===========================================================
##
##      TEST SELECTION
##
## ===========================================================



def test_selection(conn):
    assert sql.table_exists(conn, "selections")
    assert sql.table_exists(conn, "selection_has_variant")

    #GET LIST 
    selections = list(sql.get_selections(conn))
    assert len(selections) == 1   
    "variants" in selections
 
    #GET 
    selection = sql.get_selection(conn, 1)
    assert "count" in selection.keys()
    assert "name" in selection.keys()
    assert "query" in selection.keys()

    #CREATE A SELECTION WITH VARIANTS IDS
    sql.create_selection_with_variants(conn, "my selection", [1,2])
    selections = list(sql.get_selections(conn))
    assert selections[1]["name"] == "my selection"
    assert selections[1]["count"] == 2
    assert conn.execute("SELECT COUNT(*) FROM selection_has_variant WHERE selection_id = 2").fetchone()[0] == 2

    #CREATE A  SELECTION FROM INTERVAL  
    #sql.create_selection_from_interval(conn,"my selection",  "new selection", [{"chrom":"chr1","start":8,"end":29, "name":"region"}])


    # Test Insert selection 
    rowid = sql.insert_selection_from_sql(conn, "SELECT id FROM variants WHERE pos == 45","my selection")  
  
    assert rowid == 3
    selection = sql.get_selection(conn, 2)
    assert selection["id"] == 2
    assert selection["name"] == "my selection"
    assert selection["count"] == 2

    # Check selection_has_variants
    assert conn.execute("SELECT COUNT(*) FROM selection_has_variant WHERE selection_id = 2").fetchone()[0] == 2

    # Test edit selection 
    sql.update_selection(conn, {"id": 2, "name": "new name"})
    selection = sql.get_selection(conn, 2)
    assert selection["name"] == "new name"

    # Test delete selection 
    sql.delete_selection(conn, 2)
    assert len(list(sql.get_selections(conn))) == 2

    # delete selection by name 
    sql.insert_selection_from_sql(conn, "SELECT id FROM variants WHERE pos == 45","other")

    sql.delete_selection_by_name(conn,"variants")    

    sql.delete_selection_by_name(conn,"other")    
    assert len(list(sql.get_selections(conn))) == 2



## ===========================================================
##
##      FIELD TABLES 
##
## ===========================================================

def test_fields(conn):
    assert sql.table_exists(conn, "fields")

    # test get_fields 
    fields = list(sql.get_fields(conn))
    assert len(fields) == len(FIELDS)
    for index, f in enumerate(sql.get_fields(conn)):
        rowid = f.pop("id")
        assert f == FIELDS[index]
        assert index + 1 == rowid

    # test get field 
    field = sql.get_field(conn, 1 )
    del field["id"] 
    assert field  == FIELDS[0]

    # test insert fields 
    rowid = sql.create_field(conn, name="test",category="annotations",type="int", description="truc")
    fields = list(sql.get_fields(conn))
    assert len(fields) == len(FIELDS) + 1

    # test delete field 
    sql.delete_field(conn, rowid)
    fields = list(sql.get_fields(conn))
    assert len(fields) == len(FIELDS) 

## ===========================================================
##
##      Test Annotations
##
## ===========================================================

def test_get_annotations(conn):
    for id, variant in enumerate(VARIANTS):
        read_tx = list(sql.get_annotations(conn, id + 1))[0]
        del read_tx["variant_id"]
        expected_tx = VARIANTS[id]["annotations"][0]
        assert read_tx == expected_tx


def test_get_sample_annotations(conn):
    # TODO
    pass



## ===========================================================
##
##      Test Samples
##
## ===========================================================

def test_get_samples(conn):
    assert [sample["name"] for sample in sql.get_samples(conn)] == SAMPLES
    first_sample = list(sql.get_samples(conn))[0]

    #  test default value
    assert first_sample["name"] == "sacha"
    assert first_sample["fam"] == "fam"
    assert first_sample["father_id"] == 0
    assert first_sample["mother_id"] == 0
    assert first_sample["sexe"] == 0
    assert first_sample["phenotype"] == 0


def test_edit_samples(conn):
    previous_sample = list(sql.get_samples(conn))[0]

    assert previous_sample["name"] == "sacha"
    assert previous_sample["id"] == 1
    #  Update with info
    previous_sample["name"] = "maco"
    previous_sample["fam"] = "fam2"
    previous_sample["father_id"] = 1
    previous_sample["mother_id"] = 1
    previous_sample["sexe"] = 2
    previous_sample["phenotype"] = 2

    sql.update_sample(conn, previous_sample)

    edit_sample = list(sql.get_samples(conn))[0]

    assert edit_sample["name"] == "maco"
    assert edit_sample["fam"] == "fam2"
    assert edit_sample["father_id"] == 1
    assert edit_sample["mother_id"] == 1
    assert edit_sample["sexe"] == 2
    assert edit_sample["phenotype"] == 2


## ===========================================================
##
##      Test Variants
##
## ===========================================================

def test_variant(conn):


    # test get variants 



    updated = {"id": 1, "ref": "A", "chr": "chrX"}
    sql.update_variant(conn, updated)

    inserted = sql.get_variant(conn, 1)

    assert inserted["ref"] == updated["ref"]
    assert inserted["chr"] == updated["chr"]


# def test_selections(conn):
#     """Test the creation of a full selection in "selection_has_variant"
#     and "selections" tables; Test also the ON CASCADE deletion of rows in
#     "selection_has_variant" when a selection is deleted.
#     """

#     # Create a selection that contains all 8 variants in the DB
#     # (no filter on this list, via annotation table because this table is not
#     # initialized here)
#     query = """SELECT variants.id,chr,pos,ref,alt FROM variants"""
#     #    LEFT JOIN annotations
#     #     ON annotations.variant_id = variants.rowid"""

#     # Create a new selection (a second one, since there is a default one during DB creation)
#     # ret = sql.create_selection_from_sql(conn, query, "selection_name", count=None)
#     # assert ret == 2

#     # # Query the association table (variant_id, selection_id)
#     # data = conn.execute("SELECT * FROM selection_has_variant")
#     # expected = ((1, ret), (2, ret), (3, ret), (4, ret), (5, ret), (6, ret), (7, ret), (8, ret))
#     # record = tuple([tuple(i) for i in data])

#     # # Is the association table 'selection_has_variant' ok ?
#     # assert record == expected

#     # # Test ON CASCADE deletion
#     # cursor = conn.cursor()
#     # cursor.execute("DELETE FROM selections WHERE rowid = ?", str(ret))

#     # assert cursor.rowcount == 1

#     # # Now the table must be empty
#     # data = conn.execute("SELECT * FROM selection_has_variant")
#     # expected = tuple()
#     # record = tuple([tuple(i) for i in data])

#     # assert record == expected

#     # # Extra tests on transactions states
#     # assert conn.in_transaction == True
#     # conn.commit()
#     # assert conn.in_transaction == False


# def test_selection_operation(conn):
#     """test set operations on selections
#     PS: try to handle precedence of operators"""

#     #  Select all
#     query = """SELECT variants.id,chr,pos,ref,alt FROM variants"""
#     id_all = sql.create_selection_from_sql(conn, query, "all", count=None)

#     #  Select only ref = C (4 variants)
#     query = """SELECT variants.id,chr,pos,ref,alt FROM variants WHERE ref='C'"""
#     id_A = sql.create_selection_from_sql(conn, query, "setA", count=None)

#     # Select only alt = C (2 variants among setA)
#     query = """SELECT variants.id,chr,pos,ref,alt FROM variants WHERE alt='C'"""
#     id_B = sql.create_selection_from_sql(conn, query, "setB", count=None)

#     # assert all((id_all, id_A, id_B))

#     # selections = [selection["name"] for selection in sql.get_selections(conn)]

#     # assert "setA" in selections
#     # assert "setB" in selections

#     # sql.Selection.conn = conn

#     # All = sql.Selection.from_selection_id(id_all)
#     # A = sql.Selection.from_selection_id(id_A)
#     # B = sql.Selection.from_selection_id(id_B)

#     # # 8 - (4 & 2) = 8 - 2 = 6
#     # C = All - (B&A)
#     # # Query:
#     # # SELECT variant_id
#     # #    FROM selection_has_variant sv
#     # #     WHERE sv.selection_id = 2 EXCEPT SELECT * FROM (SELECT variant_id
#     # #    FROM selection_has_variant sv
#     # #     WHERE sv.selection_id = 4 INTERSECT SELECT variant_id
#     # #    FROM selection_has_variant sv
#     # #     WHERE sv.selection_id = 3)

#     # C.save("newset")

#     # print(A.sql_query)
#     # expected_number = 0
#     # for expected_number, variant in enumerate(conn.execute(A.sql_query), 1):
#     #     print(dict(variant))

#     # assert expected_number == 4

#     # print(B.sql_query)
#     # expected_number = 0
#     # for expected_number, variant in enumerate(conn.execute(B.sql_query), 1):
#     #     print(dict(variant))

#     # assert expected_number == 2

#     # print(C.sql_query)
#     # expected_number = 0
#     # for expected_number, variant in enumerate(conn.execute(C.sql_query), 1):
#     #     print(dict(variant))

#     # assert expected_number == 6

#     # selections = [selection["name"] for selection in sql.get_selections(conn)]
#     # "newset" in selections

#     # # (8 - 2) & 4 = 2
#     # C = (All - B) & A
#     # # Query:
#     # # SELECT * FROM (SELECT variant_id
#     # #        FROM selection_has_variant sv
#     # #         WHERE sv.selection_id = 2 EXCEPT SELECT variant_id
#     # #        FROM selection_has_variant sv
#     # #         WHERE sv.selection_id = 4 INTERSECT SELECT variant_id
#     # #        FROM selection_has_variant sv
#     # #         WHERE sv.selection_id = 3)
#     # print(C.sql_query)
#     # expected_number = 0
#     # for expected_number, variant in enumerate(conn.execute(C.sql_query), 1):
#     #     print(dict(variant))

#     # assert expected_number == 2


#  ============ TEST VARIANTS QUERY


# def test_select_variant_items(conn):
#     args = {}
#     # assert len(list(sql.SelectVariant(conn, **args).items())) == len(VARIANTS)

#     # args = {"filters": filters}
#     # assert len(list(sql.get_variants(conn, **args))) == 1

#     #  TODO more test


# def test_selection_from_bedfile(conn):
#     """Test the creation of a selection based on BED data

#     .. note:: Please note that the bedreader **is not** tested here!
#     """

#     larger_string = """
#         chr1 1    10   feature1  0 +
#         chr1 50   60   feature2  0 -
#         chr1 51 59 another_feature 0 +
#     """
#     # 1: chr1, pos 1 to 10 => 2 variants
#     # 2: chr1, pos 50 to 60 => 2 variants
#     # 3: chr1, pos 51 to 59 => 0 variants

#     bedtool = BedTool(larger_string)

#     # Create a new selection (a second one, since there is a default one during DB creation)
#     ret = sql.create_selection_from_bed(conn, "variants", "bedname", bedtool)

#     # # Test last id of the selection
#     # assert ret == 2

#     # # Query the association table (variant_id, selection_id)
#     # data = conn.execute("SELECT * FROM selection_has_variant WHERE selection_id = ?", (ret,))
#     # # 4 variants (see above)
#     # expected = ((1, ret), (2, ret), (6, ret), (7, ret))
#     # record = tuple([tuple(i) for i in data])

#     # # Is the association table 'selection_has_variant' ok ?
#     # assert record == expected

#     # bed_selection  = [s for s in sql.get_selections(conn) if s["name"] == "bedname"][0]
#     # assert bed_selection["name"] == "bedname"
#     # assert bed_selection["count"] == 4


# def test_selection_from_bedfile_and_subselection(conn):
#     """Test the creation of a selection based on BED data

#     .. note:: Please note that the bedreader **is not** tested here!
#     """

#     larger_string = """
#         chr1 1    10   feature1  0 +
#         chr1 50   60   feature2  0 -
#         chr1 51 59 another_feature 0 +
#     """
#     # 1: chr1, pos 1 to 10 => 2 variants
#     # 2: chr1, pos 50 to 60 => 2 variants
#     # 3: chr1, pos 51 to 59 => 0 variants

#     bedtool = BedTool(larger_string)

#     #  Create now a sub selection

#     # query = """SELECT variants.id,chr,pos,ref,alt FROM variants WHERE ref='C'"""
#     # set_A_id = sql.create_selection_from_sql(conn, query, "setA", count=None)

#     # assert "setA" in list(s["name"] for s in sql.get_selections(conn))

#     # # 1: chr1, pos 1 to 10 => 1 variants
#     # # 2: chr1, pos 50 to 60 => 2 variants
#     # # 3: chr1, pos 51 to 59 => 2 variants

#     # ret = sql.create_selection_from_bed(conn,"setA", "sub_bedname", bedtool)

#     # data = conn.execute("SELECT * FROM selection_has_variant WHERE selection_id = ?", (ret,))
#     # expected = ((2, ret), (6, ret), (7, ret))
#     # record = tuple([tuple(i) for i in data])
#     # assert record == expected


# # def test_selection_operation(conn):

# #     #  Prepare base
# #     prepare_base(conn)
# #     cursor = conn.cursor()

# #     all_selection = cursor.execute("SELECT * FROM selections").fetchone()

# #     print("all", all_selection)
# #     assert all_selection[0] == "all"
# #     assert all_selection[1] == len(variants)

# #     #  Create a selection from sql
# #     query = "SELECT chr, pos FROM variants where alt = 'A' "
# #     sql.create_selection_from_sql(conn, "test", query)

# #     # check if selection has been created
# #     assert "test" in [record["name"] for record in sql.get_selections(conn)]

# #     #  Check if selection of variants returns same data than selection query
# #     selection_id = 2
# #     insert_data = cursor.execute(query).fetchall()

# #     read_data = cursor.execute(
# #         f"""
# #         SELECT variants.chr, variants.pos FROM variants
# #         INNER JOIN selection_has_variant sv ON variants.rowid = sv.variant_id AND sv.selection_id = {selection_id}
# #         """
# #     ).fetchall()

# #     # set because, it can contains duplicate variants
# #     assert set(read_data) == set(insert_data)

# #     # TEST Unions
# #     query1 = "SELECT chr, pos FROM variants where alt = 'G' "
# #     query2 = "SELECT chr, pos FROM variants where alt = 'T' "

# #     union_query = sql.union_variants(query1, query2)
# #     print(union_query)
# #     sql.create_selection_from_sql(conn, "union_GT", union_query)
# #     record = cursor.execute(
# #         f"SELECT rowid, name FROM selections WHERE name = 'union_GT'"
# #     ).fetchone()
# #     selection_id = record[0]
# #     selection_name = record[1]
# #     assert selection_id == 3  # test if selection id equal 2 ( the first is "variants")
# #     assert selection_name == "union_GT"

# #     # Select statement from union_GT selection must contains only variant.alt G or T
# #     records = cursor.execute(
# #         f"""
# #         SELECT variants.chr, variants.pos, variants.ref, variants.alt FROM variants
# #         INNER JOIN selection_has_variant sv ON variants.rowid = sv.variant_id AND sv.selection_id = {selection_id}
# #         """
# #     ).fetchall()

# #     for record in records:
# #         assert record[3] in ("G", "T")

# #     # Todo : test intersect and expect


# def test_variants(conn):
#     pass
#     """Test that we have all inserted variants in the DB"""

#     # for i, record in enumerate(conn.execute("SELECT * FROM variants")):
#     #     record = list(record) # omit id
#     #     expected_variant = variants[i]
#     #     del expected_variant["annotations"]

#     #     assert tuple(record[1:]) == tuple(expected_variant.values())
