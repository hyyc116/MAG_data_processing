#coding:utf-8
from basic_config import *


def fetch_paper_field():

    sql = 'select paper_id,A.field_of_study_id from mag_core.paper_fields_of_study as A ,mag_core.fields_of_study as B where A.field_of_study_id=B.field_of_study_id and B.level=0'

    query_op = dbop()

    pid_field = {}

    progress = 0
    for paper_id, fos_id in query_op.query_database(sql):

        progress += 1

        pid_field[paper_id] = fos_id

        if progress % 1000000 == 0:
            logging.info(f'proress {progress} ...')

    open('data/pid_subject.json', 'w').write(json.dumps(pid_field))
    logging.info(
        f'{len(pid_field)} paper has field label, and saved to data/pid_subject.json'
    )


if __name__ == '__main__':
    fetch_paper_field()
