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


def fetch_expon_index():
    sql = 'select paper_id,A.field_of_study_id from mag_core.paper_fields_of_study as A ,mag_core.fields_of_study as B where A.field_of_study_id=B.field_of_study_id and B.level=1'

    query_op = dbop()

    fos_num = defaultdict(int)

    progress = 0
    for paper_id, fos_id in query_op.query_database(sql):

        progress += 1

        fos_num[fos_id] += 1

        if progress % 1000000 == 0:
            logging.info(f'proress {progress} ...')

    open('data/field_num.json', 'w').write(json.dumps(fos_num))
    logging.info(f'{len(fos_num)} saved to data/field_num.json')

    fig, ax = plt.subplots(1, 1, figsize=(4.5, 3.5))

    plot_field_dis(fos_num, ax)

    plt.tight_layout()

    plt.savefig('fig/level1_field_dis.png', dpi=400)
    logging.info('fig saved to fig/level1_field_dis.png')


def plot_field_dis(fos_num, ax):

    xs = []
    ys = []

    for i, fos in enumerate(
            sorted(fos_num.keys(), key=lambda x: fos_num[x], reverse=True)):

        xs.append(i + 1)
        ys.append(fos_num[fos])

    ax.plot(xs, ys)

    ax.set_yscale('log')

    ax.set_xlabel('field rank')

    ax.set_ylabel('number of publications')


def fetch_field_cits():

    logging.info('loading paper field ...')
    paper_field = json.loads(open('data/pid_subject.json').read())

    logging.info('loading paper year ...')
    paper_year = json.loads(open('data/pid_pubyear.json').read())

    sql = 'select paper_id,paper_reference_id from mag_core.paper_references'

    paper_field_citnum = defaultdict(lambda: defaultdict(int))

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

        if year1 > 2010 or year2 > 2010:
            continue

        fos1_fos2_refnum[fos1][fos2] += 1

        if year2 - year1 <= 5:
            paper_field_citnum[paper_reference_id][fos1] += 1

    open('data/fos1_fos2_refnum.json', 'w').write(json.dumps(fos1_fos2_refnum))
    logging.info('refnum data saved.')

    open('data/paper_field_citnum.json',
         'w').write(json.dumps(paper_field_citnum))

    #### 42,420,293 1971~2010年 发表后10年内
    logging.info(
        f'{len(paper_field_citnum)} papers has citations in ten years after published.'
    )


# 画出数据集内文章数量随着时间的变化，
# 各个领域的文章数量分布
def field_paper_dis():
    logging.info('loading paper field ...')
    paper_field = json.loads(open('data/pid_subject.json').read())

    logging.info('loading paper year ...')
    paper_year = json.loads(open('data/pid_pubyear.json').read())

    logging.info('start to plotting ....')

    year_counter = Counter(paper_year.values())

    # fig1
    fig1, axes = plt.subplots(1, 2, figsize=(9, 3.5))

    ax = axes[0]
    xs = []
    ys = []
    for year in sorted(year_counter.keys(), key=lambda x: int(x)):

        xs.append(int(year))
        ys.append(year_counter[year])

    ax.plot(xs, ys)

    ax.plot([1970] * 10,
            np.linspace(np.min(ys), np.max(ys), 10),
            '--',
            label='1970')

    ax.plot([2010] * 10,
            np.linspace(np.min(ys), np.max(ys), 10),
            '--',
            label='2010')

    ax.plot([2016] * 10,
            np.linspace(np.min(ys), np.max(ys), 10),
            '--',
            label='2016')

    ax.set_xlabel('year')
    ax.set_ylabel('number of publications')

    ax.set_yscale('log')

    ax.legend()

    ## 每一个领域论文的数量
    field_num = defaultdict(int)
    for paper in paper_field.keys():

        year = int(paper_year.get(paper, 1900))

        if year < 1971 or year > 2010:
            continue

        field_num[paper_field[paper]] += 1

    field_xs = []
    field_ys = []
    for field in sorted(field_num.keys(),
                        key=lambda x: field_num[x],
                        reverse=True):
        field_xs.append(field)
        field_ys.append(field_num[field])

    ax = axes[1]

    ax.plot(np.array(range(len(field_xs))) + 1, field_ys)

    ax.set_xlabel('field ID')
    ax.set_ylabel('number of publications')

    ax.set_yscale('log')

    plt.tight_layout()

    plt.savefig('fig/fig1.png', dpi=400)
    logging.info('fig saved to fig/fig1.png')


# 计算每一篇论文对各个学科的转化率
def cal_ITR():
    # 一篇论文在各个领域的
    paper_field_citnum = json.loads(
        open('data/paper_field_citnum.json').read())

    pass


if __name__ == '__main__':
    # fetch_paper_field()

    fetch_field_cits()

    # field_paper_dis()
    # fetch_expon_index()
