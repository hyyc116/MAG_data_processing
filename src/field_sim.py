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

    paper_field_citnum = defaultdict(lambda: defaultdict(int))

    fos1_fos2_refnum = defaultdict(lambda: defaultdict(int))
    query_op = dbop()
    process = 0
    for paper_id, paper_reference_id in query_op.query_database(sql):

        process += 1
        if process % 100000000 == 0:
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
    # 一篇论文在各个领域的引用次数
    logging.info('loading paper field citnum ...')
    paper_field_citnum = json.loads(
        open('data/paper_field_citnum.json').read())

    logging.info('loading paper subject ...')
    paper_subject = json.loads(open('data/pid_subject.json').read())

    logging.info('loading fos fos refnum ...')
    fos1_fos2_refnum = json.loads(open('data/fos1_fos2_refnum.json').read())

    fos1_fos2_func = defaultdict(dict)

    lines = ['fos1,fos2,func']

    for fos1 in fos1_fos2_refnum.keys():

        fos2_refnum = fos1_fos2_refnum[fos1]

        total_refnum = float(np.sum([float(i) for i in fos2_refnum.values()]))

        for fos2 in fos2_refnum.keys():

            refnum = fos2_refnum[fos2]

            func = refnum / total_refnum

            line = f'{fos2},{fos1},{func}'

            fos1_fos2_func[fos2][fos1] = func

    open('data/fos1_fos2_func.json', 'w').write(json.dumps(fos1_fos2_func))
    logging.info('fos fos cit sim saved to data/fos1_fos2_func.json')

    open('data/fos1_fos2_func.csv', 'w').write('\n'.join(lines))

    lines = ['pid,subject,Other Subject,I0,It,func,ITR']

    for paper in paper_field_citnum.keys():

        subject = paper_subject[paper]

        I0 = paper_field_citnum[paper][subject]

        for os in paper_field_citnum[paper].keys():

            if os == subject:
                continue

            It = paper_field_citnum[paper][os]

            func = fos1_fos2_func[subject][os]

            ITR = It / float(I0)

            line = f'{paper},{subject},{os},{func},{I0},{It},{ITR}'

            lines.append(line)

    open('data/paper_ITR.csv', 'w').write('\n'.join(lines))
    logging.info("data saved to data/paper_ITR.csv")


if __name__ == '__main__':
    # fetch_paper_field()

    # fetch_field_cits()

    cal_ITR()

    # field_paper_dis()
    # fetch_expon_index()
