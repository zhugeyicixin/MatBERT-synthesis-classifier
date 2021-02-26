from pymongo import HASHED, ASCENDING, DESCENDING

from synthesis_classifier.database.patents import PatentsDBWriter, get_connection
from synthesis_classifier.multiprocessing_classifier import perform_collection, make_batch


class PatentBatteryParagraphs(object):
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
        cursor = self.db.patent_battery.aggregate([
            {'$lookup': {
                'from': 'patent_text_section',
                'localField': 'patent_id',
                'foreignField': 'patent_id',
                'as': 'p'}},
            {'$unwind': '$p'},
            {'$lookup': {
                'from': self.meta_col_name,
                'localField': 'p._id',
                'foreignField': 'paragraph_id',
                'as': 'meta'}},
            {'$match': {'meta': {'$size': 0}}},
        ])

        for item in cursor:
            paragraph = item['p']['text']
            if paragraph is not None and paragraph.strip():
                yield item['p']['_id'], item['p']['text']

    def __len__(self):
        return next(self.db.patent_battery.aggregate([
            {'$lookup': {
                'from': 'patent_text_section',
                'localField': 'patent_id',
                'foreignField': 'patent_id',
                'as': 'p'}},
            {'$unwind': '$p'},
            {'$lookup': {
                'from': self.meta_col_name,
                'localField': 'p._id',
                'foreignField': 'paragraph_id',
                'as': 'meta'}},
            {'$match': {'meta': {'$size': 0}}},
            {'$count': 'total'}
        ]))['total']


if __name__ == "__main__":
    batch_size = 16
    meta_col_name = 'paragraph_battery_fulltext_meta'
    perform_collection(
        PatentsDBWriter(meta_col_name=meta_col_name),
        make_batch(
            PatentBatteryParagraphs(meta_col_name=meta_col_name),
            batch_size
        ),
        job_script=None
    )
