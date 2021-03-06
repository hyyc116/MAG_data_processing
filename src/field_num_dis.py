#coding:utf-8
from basic_config import *


# 领域数量分布
def fetch_expon_index():

    query_op = dbop()

    sql = 'select field_of_study_id,display_name,level from mag_core.fields_of_study where level = 0 or level=1'

    level0_foses = []
    lines = ['fosid,name,level']
    for fos, name, level in query_op.query_database(sql):

        if level == 0:
            level0_foses.append(fos)

        line = f'{fos},{name},{level}'
        lines.append(line)

    open('data/fos_level0.txt', 'w').write('\n'.join(lines))

    sql = 'select field_of_study_id,child_field_of_study_id from mag_core.fields_of_study_children'

    fos_childrens = defaultdict(list)
    for fosid, cfosid in query_op.query_database(sql):

        if fosid in level0_foses:

            fos_childrens[fosid].append(cfosid)

    open('data/level0_level1s.json', 'w').write(json.dumps(fos_childrens))

    sql = 'select paper_id,A.field_of_study_id from mag_core.paper_fields_of_study as A ,mag_core.fields_of_study as B where A.field_of_study_id=B.field_of_study_id and B.level=0'
    progress = 0
    level0_num = defaultdict(int)
    for paper_id, fos_id in query_op.query_database(sql):

        progress += 1

        level0_num[fos_id] += 1

        if progress % 1000000 == 0:
            logging.info(f'proress {progress} ...')

    open('data/field_level0_num.json', 'w').write(json.dumps(level0_num))
    logging.info(
        f'{len(level0_num)} papers saved to data/field_level0_num.json')

    sql = 'select paper_id,A.field_of_study_id from mag_core.paper_fields_of_study as A ,mag_core.fields_of_study as B where A.field_of_study_id=B.field_of_study_id and B.level=1'
    level1_num = defaultdict(int)
    progress = 0
    for paper_id, fos_id in query_op.query_database(sql):

        progress += 1

        level1_num[fos_id] += 1

        if progress % 1000000 == 0:
            logging.info(f'proress {progress} ...')

    open('data/field_level1_num.json', 'w').write(json.dumps(level1_num))
    logging.info(f'{len(level1_num)} saved to data/field_num.json')


def plot_num_dis():

    level0_num = json.loads(open('data/field_level0_num.json').read())
    level1_num = json.loads(open('data/field_level1_num.json').read())
    fos_childrens = json.loads(open('data/level0_level1s.json').read())

    fos_name = {}
    for line in open('data/fos_level0.txt'):

        line = line.strip()

        if line.startswith('fos'):
            continue

        fos, name, level = line.split(',')

        fos_name[fos] = name

    logging.info(f'{len(fos_childrens)} level 0 field has children.')

    fig, axes = plt.subplots(5, 4, figsize=(18, 20))

    ax = axes[0][0]
    plot_field_dis(level0_num, ax)
    ax.set_title('level 0 dis')

    for i, fos in enumerate(fos_childrens.keys()):
        index = i + 1
        ax = axes[index // 4][index % 4]

        new_level1_num = {}

        for level1 in fos_childrens[fos]:
            num = level1_num.get(level1, None)
            if num is None:
                continue
            new_level1_num[level1] = num

        plot_field_dis(new_level1_num, ax)

        ax.set_title(fos_name[fos])
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

    expfunc = lambda t, a, b: a * np.exp(b * t)
    index_xs = np.arange(len(xs))
    fit_ys = np.array(ys) / float(np.sum(ys))
    popt, pcov = scipy.optimize.curve_fit(expfunc,
                                          index_xs,
                                          fit_ys,
                                          p0=(0.2, -0.2))
    ax.plot(np.array(index_xs), fit_ys)
    ax.plot(index_xs, [expfunc(x, *popt) for x in index_xs],
            '--',
            label=u'Fitted Curve: $p(n)=%.2f*e^{%.2fn}$' % (popt[0], popt[1]),
            c='r')

    ax.set_yscale('log')

    ax.set_xlabel('field rank')

    ax.set_ylabel('number of publications')

    ax.legend()


if __name__ == '__main__':
    # fetch_expon_index()
    plot_num_dis()