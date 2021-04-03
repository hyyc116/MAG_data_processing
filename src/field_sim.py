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


def fetch_field_cits():

    logging.info('loading paper field ...')
    paper_field = json.loads(open('data/pid_subject.json').read())

    logging.info('loading paper year ...')
    paper_year = json.loads(open('data/pid_pubyear.json').read())

    sql = 'select paper_id,paper_reference_id from mag_core.paper_references'

    fos1_fos2_refnum = defaultdict(lambda: defaultdict(int))
    query_op = dbop()
    process = 0
    for paper_id, paper_reference_id in query_op.query_database(sql):

        process += 1
        if process % 10000000 == 0:
            logging.info(f'progress {process} ....')

        fos1 = paper_field.get(paper_id, None)
        fos2 = paper_field.get(paper_reference_id, None)

        if fos1 is None or fos2 is None:
            continue

        year1 = int(paper_year.get(paper_id, 1900))
        year2 = int(paper_year.get(paper_reference_id, 1900))

        if year1 < 1970 or year2 < 1970:
            continue

        fos1_fos2_refnum[fos1][fos2] += 1

    open('data/fos1_fos2_refnum.json', 'w').write(json.dumps(fos1_fos2_refnum))
    logging.info('refnum data saved.')


if __name__ == '__main__':
    # fetch_paper_field()

    fetch_field_cits()
