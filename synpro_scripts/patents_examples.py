import os
import sys

parent_folder = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        '..'
    )
)
print('parent_folder', parent_folder)
if parent_folder not in sys.path:
    sys.path.append(parent_folder)

import re
from pymongo import HASHED, ASCENDING, DESCENDING

from synthesis_classifier.multiprocessing_classifier import perform_collection, make_batch
from synthesis_classifier.database.patents import PatentsDBWriter, PatentParagraphsByQuery
from synthesis_classifier.database.patents import get_connection

def example_paragraphs():
    query = {
        'path': re.compile(r'.*example.*', re.IGNORECASE),
    }

    return PatentParagraphsByQuery(query)

def create_db_index(meta_col_name):
    db = get_connection()
    db_col = db[meta_col_name]
    db_col.create_index([('paragraph_id', ASCENDING)], unique=False)
    db_col.create_index([('paragraph_id', HASHED)], unique=False)
    db_col.create_index([('classification', HASHED)], unique=False)
    db_col.create_index([('classifier_version', HASHED)], unique=False)
    db_col.create_index([('classifier_version', ASCENDING)], unique=False)
    db_col.create_index([('confidence', DESCENDING)], unique=False)


if __name__ == "__main__":
    batch_size = 16
    meta_col_name = 'paragraph_example_meta'

    create_db_index(meta_col_name=meta_col_name)

    perform_collection(
        PatentsDBWriter(meta_col_name=meta_col_name),
        make_batch(example_paragraphs(), batch_size),
        job_script=None
    )
