# Some functions anologuous to what is in cutevariant.core.sql to create and
# manipulate the clinical information table
def create_clinical_info(conn):
    """Create a table to store clinical information

    The table is basically a key/value store and has the following columns :
    id : integer primary key
    name : Full name of the field to be displayed
    type : string representing the way the field has to be displayed
    value : contents of the field

    :param conn: a sqlite3.connect connection
    """
    cursor = conn.cursor()

    query = """CREATE TABLE clinical_data
    (
        id INTEGER PRIMARY KEY,
        name TEXT,
        display_type TEXT,
        value TEXT
    )
    """
    cursor.execute(query)
    conn.commit()


def insert_clinical_info(conn, name, display_type, value):
    """Insert one clinical information field into the database table.
    create_clinical_info must have been called first

    :param conn: a sqlite3.connect connection
    :param name: display name of the field
    :param field_type: display type of the field
    :param value: value of the field
    """
    cursor = conn.cursor()
    query = """INSERT INTO clinical_data(name,display_type,value)
    VALUES(?,?,?)
    """
    cursor.execute(query, (name, display_type, value))
    conn.commit()


def get_clinical_info(conn):
    """Get the contents of the clinical information database
    Will also check if the table exists and return an empty list if it doesn't

    :param conn: a sqlite3.connect connection
    """
    res = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='clinical_data'"
    )
    if next(res)[0] == 0:
        return []
    return [dict(x) for x in conn.execute("SELECT * FROM clinical_data ORDER BY id")]
