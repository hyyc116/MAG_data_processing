#coding:utf-8
'''[数据预处理]

[将数据库中的数据常用字段存储为json文件]
'''

from basic_config import *


# 从数据库中读取数据，并进行存储
def process_pid_year_doctype():

    query_op = dbop()

    # 首先是读取 mag_core.papers
    sql = 'select paper_id,year,doc_type from mag_core.papers'

    logging.info('starting to read paper pubyear paper doctype.')

    pid_pubyear = {}
    pid_doctype = {}

    process = 0

    # 数量统计
    year_num = defaultdict(int)
    # doctype分布
    doctype_num = defaultdict(int)
    for paper_id, year, doc_type in query_op.query_database(sql):

        process += 1

        if process % 100000 == 0:
            logging.info(f'progress {process} ....')

        pid_pubyear[paper_id] = year
        pid_doctype[paper_id] = doc_type
        # 统计每年发表的论文的数量
        year_num[int(year)] += 1
        # 统计每一种文献类型的
        doctype_num[doc_type] += 1

    # 存储
    open('data/pid_pubyear.json', 'w').write(json.dumps(pid_pubyear))
    logging.info('pid year data saved to data/pid_pubyear.json.')
    open('data/pid_doctype.json', 'w').write(json.dumps(pid_doctype))
    logging.info('pid doctype data saved to data/pid_doctype.json.')

    # 画图
    years = []
    nums = []
    for year in sorted(year_num.keys(), key=lambda x: int(x)):

        years.append(int(year))
        nums.append(year_num[year])
    plt.figure(figsize=(7, 5))
    plt.plot(years, nums)
    plt.xlabel('year')
    plt.ylabel('number of publications')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig('fig/year_num.png', dpi=200)
    logging.info('year num dis fig saved to fig/year_num.png.')

    open('data/year_num.json', 'w').write(json.dumps(year_num))

    # doctype的分布

    docs = []
    nums = []

    for doc in sorted(doctype_num.keys(),
                      key=lambda x: doctype_num[x],
                      reverse=True)[:10]:

        docs.append(doc)
        nums.append(doctype_num[doc])

    plt.figure(figsize=(7, 5))

    plt.bar(range(len(docs)), nums, width=0.4)

    plt.xticks(range(len(docs)), docs)

    plt.xlabel('doctype')

    plt.ylabel('number of publications')

    plt.tight_layout()

    plt.savefig('fig/doctype_num.png', dpi=200)

    logging.info('fig saved to fig/doctype_num.png')

    open('data/doctype_num.json', 'w').write(json.dumps(doctype_num))

    logging.info('Total number of papers:{}.'.format(np.sum(
        year_num.values())))


def fetch_pid_c5():

    # ID对应的年份
    logging.info('Starting to load paper pubyear ...')
    pid_year = json.loads(open('data/pid_pubyear.json').read())

    logging.info('reading papers ...')

    query_op = dbop()

    # 首先是读取 mag_core.papers
    sql = 'select paper_id,paper_reference_id from mag_core.paper_references'

    logging.info('starting to read paper pubyear paper doctype.')

    refnum_count = defaultdict(int)
    pid_c5 = defaultdict(int)

    pid_refs = defaultdict(list)

    process = 0

    ref_lines = []
    for paper_id, paper_reference_id in query_op.query_database(sql):

        process += 1

        if process % 100000 == 0:
            logging.info(f'progress {process} ....')

        pid_refs[paper_id].append(paper_reference_id)

        citing_year = int(pid_year.get(paper_id, 9999))
        cited_year = int(pid_year.get(paper_reference_id, 9999))

        if citing_year > 2010 or cited_year > 2010:
            continue

        if abs(citing_year - cited_year) < 5:
            pid_c5[paper_reference_id] += 1

    # 存储论文五年内的被引量
    open('data/paper_c5.json', 'w').write(json.dumps(pid_c5))

    pid_FIR = {}
    # 对应每一篇论文的FIR——5
    for pid in pid_refs.keys():
        refs = pid_refs[pid]
        pv = pid_c5[pid]

        if not pv > 0:
            continue

        if len(refs) < 5 or len(refs) > 100:
            continue

        # ref_v
        ref_v = np.mean([pid_c5.get(ref, 0) for ref in refs])

        if not ref_v > 0:
            continue

        # 每一篇论文的ref_v
        FIR = float(pv) / ref_v

        pid_FIR[pid] = FIR

    # 存储每篇论文的FIR
    open('data/MAG_paper_FIR5.json', 'w').write(json.dumps(pid_FIR))
    logging.info('data saved to data/MAG_paper_FIR5.json')


# 丛数据库中将参考文献关系存储下来,先不存了 直接读吧 感觉有点多啊
def process_pid_refs():

    # 1,399,752,645 reference relations

    query_op = dbop()

    # 首先是读取 mag_core.papers
    sql = 'select paper_id,paper_reference_id from mag_core.paper_references'

    logging.info('starting to read paper pubyear paper doctype.')

    # ref_file =  open('data/pid_refs.txt','w')

    refnum_count = defaultdict(int)
    pid_cn = defaultdict(int)

    pid_refs = defaultdict(list)

    process = 0

    ref_lines = []
    for paper_id, paper_reference_id in query_op.query_database(sql):

        process += 1

        if process % 100000 == 0:
            logging.info(f'progress {process} ....')

        pid_refs[paper_id].append(paper_reference_id)

        pid_cn[paper_reference_id] += 1

    logging.info(f'total num of references {process} ...')

    # 首先是遍历存储
    saved_data = {}
    for pid in pid_refs.keys():
        refnum = len(pid_refs[pid])

        refnum_count[refnum] += 1

    # 统计参考文献数量分布

    refnums = []
    count = []

    for refnum in sorted(refnum_count.keys()):

        refnums.append(refnum)
        count.append(refnum_count[refnum])

    plt.figure(figsize=(7, 5))

    plt.plot(refnums, count)

    plt.xlabel('number of references')

    plt.ylabel('number of publications')

    plt.xscale('log')

    plt.yscale('log')

    plt.tight_layout()

    plt.savefig('fig/refnum_dis.png', dpi=200)

    open('data/refnum_count.json', 'w').write(json.dumps(refnum_count))

    logging.info('fig saved to fig/refnum_dis.png')

    # 引用次数分布
    open('data/pid_cn.json', 'w').write(json.dumps(pid_cn))

    value_counter = Counter(pid_cn.values())
    cns = []
    nums = []
    for value in sorted(value_counter.keys(), key=lambda x: int(x)):

        cns.append(value)
        nums.append(value_counter[value])

    plt.figure(figsize=(7, 5))

    plt.plot(cns, nums)

    plt.xscale('log')

    plt.yscale('log')

    plt.xlabel('number of citations')

    plt.ylabel('number of publications')

    plt.tight_layout()

    plt.savefig('fig/cn_dis.png', dpi=200)

    logging.info('fig saved to fig/cn_dis.png')


# 丛数据库中将参考文献关系存储下来,先不存了 直接读吧 感觉有点多啊
def process_pid_author():

    query_op = dbop()

    # 首先是读取 mag_core.papers
    sql = 'select paper_id,author_id,author_sequence_number from mag_core.paper_author_affiliations'

    logging.info('starting to read paper pubyear paper doctype.')

    pid_seq_author = defaultdict(dict)

    author_prod = defaultdict(int)

    process = 0

    ref_lines = []
    for paper_id, author_id, author_sequence_number in query_op.query_database(
            sql):

        process += 1

        if process % 100000 == 0:
            logging.info(f'progress {process} ....')

        pid_seq_author[paper_id][author_sequence_number] = author_id

        author_prod[author_id] += 1

    open('data/pid_seq_author.json', 'w').write(json.dumps(pid_seq_author))

    open('data/author_prod.json', 'w').write(json.dumps(author_prod))

    prods = []
    nums = []

    prod_counter = Counter(author_prod.values())

    for prod in sorted(prod_counter.keys()):

        prods.append(prod)
        nums.append(prod_counter[prod])

    plt.figure(figsize=(7, 5))

    plt.plot(prods, nums)

    plt.xscale('log')

    plt.yscale('log')

    plt.xlabel('author productivity')

    plt.ylabel('number of authors')

    plt.tight_layout()

    plt.savefig('fig/author_prod.png', dpi=200)

    print('fig saved to fig/author_prod.png.')


if __name__ == '__main__':
    # 获取论文的发布时间及文档类型
    # process_pid_year_doctype()

    # 参考文献的统计

    # process_pid_refs()

    # 作者数据

    # process_pid_author()

    # 计算作者的FIR
    fetch_pid_c5()
