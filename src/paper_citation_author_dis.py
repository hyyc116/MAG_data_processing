#coding:utf-8
'''[分析一篇论文的被引次数的作者构成]

[一篇论文被一位作者反复引用，那么这些作者引用次数的分布是怎样的]
'''
from basic_config import *

# 随机选择100篇大于1000次引用的论文，然后得到他们的被引论文的ID列表
def rand_select_papers():

    # 论文id
    pid_cn = json.loads(open('data/pid_cn.json').read())

    pid_50 = []

    pid_100 = []

    pid_500 = []

    pid_1000 = []

    for pid in pid_cn.keys():

        cn = int(pid_cn[pid])

        if cn>=1000:
            pid_1000.append(pid)
        elif cn>=500:
            pid_500.append(pid)
        elif cn>=100:
            pid_100.append(pid)
        elif cn>=50:
            pid_50.append(pid)


    selected_pids = []

    selected_pids.extend(np.random.choice(pid_1000,size=500))
    selected_pids.extend(np.random.choice(pid_500,size=500))
    selected_pids.extend(np.random.choice(pid_100,size=500))
    selected_pids.extend(np.random.choice(pid_50,size=500))

    selected_pids = set(selected_pids)

    ## 从引用关系中过滤得到这些ID的被引论文
    query_op = dbop()

    pid_cits = defaultdict(list)

    # 首先是读取 mag_core.papers
    sql = 'select paper_id,paper_reference_id from mag_core.paper_references'
    process = 0
    for paper_id,paper_reference_id in query_op.query_database(sql):
        process+=1
        if process%10000000==0:
            logging.info(f'progress {process} ....')


        if paper_reference_id in selected_pids:

            pid_cits[paper_reference_id].append(paper_id)



    open('data/selected_pid_cits.json','w').write(json.dumps(pid_cits))

    logging.info('selection done!')


    query_op.close_db()

# 统计
def stat_cit_dis():

    # 文章 作者
    pid_seq_author = json.loads(open('data/pid_seq_author.json').read())
    # 文档
    selected_pid_cits = json.loads(open('data/selected_pid_cits.json').read())

    pid_author_num = defaultdict(lambda:defaultdict(int))

    # 
    for pid in selected_pid_cits.keys():

        cits = selected_pid_cits[pid]

        for cit in cits:

            if pid_seq_author.get(cit,None) is None:
                continue

            # for author in  pid_seq_author[cit].values():
            try:
                author = pid_seq_author[cit]['1']
            except:
                print(pid_seq_author[cit])

            pid_author_num[pid][author]+=1


    fig,axes = plt.subplots(10,10,figsize=(40,35))

    pids = np.random.choice(list(pid_author_num.keys()),size=100)

    for i,pid in enumerate(sorted(pids,key=lambda x:len(selected_pid_cits[x]))):

        ax = axes[i//10][i%10]

        author_num = pid_author_num[pid]

        num_counter = Counter(author_num.values())

        nums = []

        counts = []

        for num in sorted(num_counter.keys()):

            nums.append(num)
            counts.append(num_counter[num])


        ax.plot(nums,counts)

        ax.set_title(len(selected_pid_cits[pid]))

        ax.set_xlabel('number of citations')

        ax.set_ylabel('number of authors')

        ax.set_xscale('log')
        ax.set_yscale('log')



    plt.tight_layout()

    plt.savefig('fig/t1000_100.png',dpi=200)

    logging.info('fig saved.')


if __name__ == '__main__':
    # rand_select_papers()

    stat_cit_dis()
