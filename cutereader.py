from cutevariant.core.reader import VcfReader, FakeReader
from cutevariant.core import sql, Query
import sys 
import json
import sqlite3
import os

from PySide2.QtWidgets import * 
from PySide2.QtCore import * 
from PySide2.QtGui import * 


from cutevariant.core.vql import model_from_string


from textx import metamodel_from_file
hello_meta = metamodel_from_file('cutevariant/core/vql.tx')


a = hello_meta.model_from_str("SELECT genotype(test),chr, pos FROM variants WHERE chr=3")


for i in a.select.columns:
	print(i.id)


print(model_from_string("SELECT genotype(boby),chr, pos FROM variants WHERE genotype(boby)=3"))




	# sql.create_table_variants(conn, reader.get_fields_by_category("variant"))

	# for _,_ in sql.async_insert_many_variants(conn, reader.get_variants()):
	# 	print("insert")



	# if options == "fields":
	# 	print(json.dumps(list(reader.get_fields())))

	# else: 
	# 	print(json.dumps(list(reader.get_variants())))
