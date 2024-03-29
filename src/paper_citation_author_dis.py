# coding:utf-8
'''[分析一篇论文的被引次数的作者构成]

[一篇论文被一位作者反复引用，那么这些作者引用次数的分布是怎样的]
'''
from itertools import count
from basic_config import *

import seaborn as sns

# 随机选择100篇大于1000次引用的论文，然后得到他们的被引论文的ID列表


def rand_select_papers():

    # 论文id
    pid_cn = json.loads(open('data/pid_cn.json').read())

    pid_10 = []

    pid_50 = []

    pid_100 = []

    pid_500 = []

    pid_1000 = []

    for pid in pid_cn.keys():

        cn = int(pid_cn[pid])

        if cn >= 1000:
            pid_1000.append(pid)
        elif cn >= 500:
            pid_500.append(pid)
        elif cn >= 100:
            pid_100.append(pid)
        elif cn >= 50:
            pid_50.append(pid)
        elif cn >= 10:
            pid_10.append(pid)

    selected_pids = []

    selected_pids.extend(np.random.choice(pid_1000, size=500))
    selected_pids.extend(np.random.choice(pid_500, size=500))
    selected_pids.extend(np.random.choice(pid_100, size=500))
    selected_pids.extend(np.random.choice(pid_50, size=500))
    selected_pids.extend(np.random.choice(pid_10, size=500))

    selected_pids = set(selected_pids)

    # 从引用关系中过滤得到这些ID的被引论文
    query_op = dbop()

    pid_cits = defaultdict(list)

    # 首先是读取 mag_core.papers
    sql = 'select paper_id,paper_reference_id from mag_core.paper_references'
    process = 0
    for paper_id, paper_reference_id in query_op.query_database(sql):
        process += 1
        if process % 10000000 == 0:
            logging.info(f'progress {process} ....')

        if paper_reference_id in selected_pids:

            pid_cits[paper_reference_id].append(paper_id)

    open('data/selected_pid_cits.json', 'w').write(json.dumps(pid_cits))

    logging.info('selection done!')

    query_op.close_db()


# 统计一篇论文被引用的作者引用次数分布
def stat_cit_dis():

    # 文章 作者
    pid_seq_author = json.loads(open('data/pid_seq_author.json').read())
    # 文档
    selected_pid_cits = json.loads(open('data/selected_pid_cits.json').read())

    pid_author_num = defaultdict(lambda: defaultdict(int))

    #
    for pid in selected_pid_cits.keys():

        cits = selected_pid_cits[pid]

        for cit in cits:

            if pid_seq_author.get(cit, None) is None:
                continue

            for author in pid_seq_author[cit].values():
                # try:
                # author = pid_seq_author[cit]['1']
                # except:
                # print(pid_seq_author[cit])

                pid_author_num[pid][author] += 1

    
    filtered_pids = []

    
    for pid in pid_author_num.keys():

        author_num = pid_author_num[pid]

        num_counter = Counter(author_num.values())

        if len(num_counter.keys())<4:
            continue

        filtered_pids.append(pid)

    fig, axes = plt.subplots(10, 4, figsize=(20, 40))

    sns.set_theme(style='ticks')

    pids = np.random.choice(filtered_pids, size=40)

    cn_n1s = defaultdict(list)
    cn_as = defaultdict(list)

    for i, pid in enumerate(
            sorted(pids, key=lambda x: len(selected_pid_cits[x]))):

        ax = axes[i // 4][i % 4]

        author_num = pid_author_num[pid]

        num_counter = Counter(author_num.values())

        nums = []

        counts = []

        for num in sorted(num_counter.keys()):

            nums.append(num)

            counts.append(num_counter[num])

        if len(nums) == 1:
            continue
        
        N1,A = fit_powlaw_N1(nums,counts)

        ax.plot(nums, counts, '-o', c=sns.color_palette()[1],label="$\\alpha={:.2f}$".format(A))

        cn = len(selected_pid_cits[pid])

        ax.set_title(f'#citations: {cn}')

        ax.set_xlabel('Number of reuses')

        ax.set_ylabel('Number of citing authors')

        ax.set_xscale('log')

        ax.set_yscale('log')

        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)

        ax.legend()



        N1, a = fit_powlaw_N1(nums, counts)

        cn_n1s[cn].append(N1)

        cn_as[cn].append(a)

    sns.despine()

    plt.tight_layout()

    plt.savefig('fig/t1000_100.png', dpi=200)

    logging.info('fig saved.')

    return

    xs = []
    ys = []
    for cn in sorted(cn_n1s.keys()):

        xs.append(cn)

        ys.append(np.mean(cn_n1s[cn]))

    plt.figure(figsize=(5, 4))

    plt.plot(xs, ys, 'o', fillstyle='none')

    plt.xlabel('number of citations')

    plt.ylabel('max N1')

    plt.xscale('log')

    plt.tight_layout()

    plt.savefig('fig/paper_cn_n1.png', dpi=400)

    xs = []

    ys = []

    for cn in sorted(cn_as.keys()):

        xs.append(cn)

        ys.append(np.mean(cn_as[cn]))

    plt.figure(figsize=(5, 4))

    plt.plot(xs, ys, 'o', fillstyle='none')

    plt.xlabel('number of citations')

    plt.ylabel('a')

    plt.xscale('log')

    plt.tight_layout()

    plt.savefig('fig/paper_cn_a.png', dpi=400)


def fit_powlaw_N1(nums, counts):

    print(len(nums), len(counts))

    N1 = None

    for i, num in enumerate(nums):
        if counts[i] == 1:
            N1 = num
            break

    if N1 is None:
        N1 = nums[-1]

    counts = np.array(counts) / float(np.sum(counts))

    linear_func = lambda x, a, b: a * x + b

    a, _ = scipy.optimize.curve_fit(linear_func, np.log(nums),
                                    np.log(counts))[0]

    print(N1, a)
    return N1, a


# 高产作者中 参考文献被重复引用的次数分布
def author_ref_dis():

    pid_seq_author = json.loads(open('data/pid_seq_author.json').read())

    # 作者
    author_plist = defaultdict(list)
    for pid in pid_seq_author.keys():

        for author in pid_seq_author[pid].values():

            author_plist[author].append(pid)

    # 高产作者
    t100_authors = []
    # 对应的论文
    all_plist = []
    # 选择的高被引作者
    t100_author_plist = {}

    for author in author_plist.keys():

        plist = author_plist[author]

        all_plist.extend(plist)

        if len(plist) > 10 and len(plist) < 1000:
            t100_authors.append(author)

            t100_author_plist[author] = plist

    t100_authors = set(t100_authors)
    logging.info(f'{len(t100_authors)} authors has 100-1000 papers.')

    all_plist = set(all_plist)

    open('data/t100_author_papers.json',
         'w').write(json.dumps(t100_author_plist))

    logging.info('data saved to data/t100_author_papers.json.')

    # 从引用关系中过滤得到这些ID的被引论文
    query_op = dbop()

    pid_refs = defaultdict(list)

    # 首先是读取 mag_core.papers
    sql = 'select paper_id,paper_reference_id from mag_core.paper_references'
    process = 0
    for paper_id, paper_reference_id in query_op.query_database(sql):
        process += 1
        if process % 100000000 == 0:
            logging.info(f'progress {process} ....')

        if paper_id in all_plist:

            pid_refs[paper_id].append(paper_reference_id)

    # 存下来
    open('data/selected_paper_refs.json', 'w').write(json.dumps(pid_refs))
    logging.info("data saved to data/selected_paper_refs.json.")


# 随机选择100位作者 将他们的参考文献引用次数分布画出来


def plot_author_ref_dis():
    # 生产力大于100的作者的论文
    t100_author_papers = json.loads(
        open('data/t100_author_papers.json').read())
    #  论文参考文献关系
    paper_refs = json.loads(open('data/selected_paper_refs.json').read())


    ## 10~20, 20~50,50~100,100~200,200+
    filtered_20_authors = []
    filtered_50_authors = []
    filtered_100_authors = []
    filtered_200_authors = []
    filtered_500_authors = []
    filtered_1000_authors = []



    # 作者的数量
    for i, author in enumerate(t100_author_papers.keys()):
        papers = t100_author_papers[author]

        pnum = len(papers)

        ref_count = defaultdict(int)
        # 作者的论文，查看查考文献
        for paper in papers:

            if paper_refs.get(paper, None) is None:
                continue

            for ref in paper_refs[paper]:

                ref_count[ref] += 1

        refnum_counter = Counter(ref_count.values())

        if len(refnum_counter.keys())<4:
            continue

        if pnum<20:
            filtered_20_authors.append(author)
        
        elif pnum<50:
            filtered_50_authors.append(author)
        
        elif pnum<100:
            filtered_100_authors.append(author)

        elif pnum<200:
            filtered_200_authors.append(author)
        elif pnum<500:
            filtered_500_authors.append(author)
        else:
            filtered_1000_authors.append(author)
        
        
    filtered_authors = [] 

    filtered_authors.extend(np.random.choice(filtered_20_authors, size=40))
    filtered_authors.extend(np.random.choice(filtered_50_authors, size=40))
    filtered_authors.extend(np.random.choice(filtered_100_authors, size=40))
    filtered_authors.extend(np.random.choice(filtered_200_authors, size=40))
    filtered_authors.extend(np.random.choice(filtered_500_authors, size=40))
    filtered_authors.extend(np.random.choice(filtered_1000_authors, size=40))


    # 随机选取100位作者,查看他们的引用次数分布情况
    authors = np.random.choice(filtered_authors, size=40)

    fig, axes = plt.subplots(10, 4, figsize=(20, 40))

    sns.set_theme(style='ticks')

    # 作者的数量
    for i, author in enumerate(sorted(authors,key=lambda x:len(t100_author_papers[x]))):
        papers = t100_author_papers[author]

        ax = axes[i // 4][i % 4]

        ref_count = defaultdict(int)
        # 作者的论文，查看查考文献
        for paper in papers:

            if paper_refs.get(paper, None) is None:
                continue

            for ref in paper_refs[paper]:

                ref_count[ref] += 1

        refnum_counter = Counter(ref_count.values())

        xs = []
        ys = []
        for refnum in sorted(refnum_counter.keys()):

            xs.append(refnum)

            ys.append(refnum_counter[refnum])
        
        N1,alpha =  fit_powlaw_N1(xs, ys)

        ax.plot(xs, ys, '-o',label = "$\\alpha={:.2f}$".format(alpha))

        ax.set_title(f'#pubs: {len(papers)}')

        ax.set_xlabel('Number of reuses')

        ax.set_ylabel('Number of cited publications')

        ax.tick_params(axis='x', labelsize=10)
        ax.tick_params(axis='y', labelsize=10)

        ax.set_xscale('log')

        ax.set_yscale('log')

        ax.legend()


    sns.despine()
    plt.tight_layout()

    plt.savefig('fig/paper_ref_cit_dis.png', dpi=200)
    logging.info('fig saved to fig/paper_ref_cit_dis.png.')


def fit_powlaw_N1(nums, counts):
    # print(len(nums), len(counts))

    N1 = None
    for i, num in enumerate(nums):

        N1 = num
        if counts[i] == 1:
            break

    counts = np.array(counts) / float(np.sum(counts))

    def linear_func(x, a, b):
        return a * x + b

    a, _ = scipy.optimize.curve_fit(linear_func, np.log(nums),
                                    np.log(counts))[0]

    return N1, a

if __name__ == '__main__':
    # rand_select_papers()

    stat_cit_dis()

    # author_ref_dis()
    plot_author_ref_dis()
