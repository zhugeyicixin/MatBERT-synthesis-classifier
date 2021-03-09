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

from synthesis_classifier.database.patents import PatentsDBWriter, get_connection
from synthesis_classifier.multiprocessing_classifier import perform_collection, make_batch


class PatentExampleParagraphs(object):
    def __init__(self, meta_col_name):
        self.db = get_connection()
        self.meta_col_name = meta_col_name
        db_col = self.db[self.meta_col_name]
        db_col.create_index([('paragraph_id', ASCENDING)], unique=False)
        db_col.create_index([('paragraph_id', HASHED)], unique=False)
        db_col.create_index([('classification', HASHED)], unique=False)
        db_col.create_index([('classifier_version', HASHED)], unique=False)
        db_col.create_index([('classifier_version', ASCENDING)], unique=False)
        db_col.create_index([('confidence', DESCENDING)], unique=False)


    def __iter__(self):
        cursor = self.db.patent_text_section.aggregate([
            {'$match': {
                    'path': re.compile(r'.*example.*', re.IGNORECASE),
                }
            },
            {'$lookup': {
                'from': self.meta_col_name,
                'localField': '_id',
                'foreignField': 'paragraph_id',
                'as': 'meta'}},
            {'$match': {'meta': {'$size': 0}}},
        ])

        for item in cursor:
            paragraph = item['text']
            if paragraph is not None and paragraph.strip():
                yield item['_id'], item['text']


    def __len__(self):
        return next(self.db.patent_battery.aggregate([
            {'$match': {
                    'path': re.compile(r'.*example.*', re.IGNORECASE),
                }
            },
            {'$lookup': {
                'from': self.meta_col_name,
                'localField': '_id',
                'foreignField': 'paragraph_id',
                'as': 'meta'}},
            {'$match': {'meta': {'$size': 0}}},
            {'$count': 'total'},
        ]))['total']


if __name__ == "__main__":
    batch_size = 16
    meta_col_name = 'paragraph_example_meta'
    perform_collection(
        PatentsDBWriter(meta_col_name=meta_col_name),
        make_batch(
            PatentExampleParagraphs(meta_col_name=meta_col_name),
            batch_size
        ),
        job_script=None
    )
