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
    for paper_id,year,doc_type in query_op.query_database(sql):

        process+=1

        if process%100000==0:
            logging.info(f'progress {process} ....')

        pid_pubyear[paper_id] = year
        pid_doctype[paper_id] = doc_type
        # 统计每年发表的论文的数量
        year_num[int(year)]+=1
        # 统计每一种文献类型的
        doctype_num[doc_type]+=1

    # 存储
    open('data/pid_pubyear.json','w').write(json.dumps(pid_pubyear))
    logging.info('pid year data saved to data/pid_pubyear.json.')
    open('data/pid_doctype.json','w').write(json.dumps(pid_doctype))
    logging.info('pid doctype data saved to data/pid_doctype.json.')


    # 画图
    years = []
    nums = []
    for year in sorted(year_num.keys(),key=lambda x:int(x)):

        years.append(int(year))
        nums.append(year_num[year])
    plt.figure(figsize=(7,5))
    plt.plot(years,nums)
    plt.xlabel('year')
    plt.ylabel('number of publications')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig('fig/year_num.png',dpi=200)
    logging.info('year num dis fig saved to fig/year_num.png.')

    open('data/year_num.json','w').write(json.dumps(year_num))

    # doctype的分布

    docs = []
    nums = []

    for doc in sorted(doctype_num.keys(),key=lambda x:doctype_num[x],reverse= True)[:10]:

        docs.append(doc)
        nums.append(doctype_num[doc])

    plt.figure(figsize=(7,5))

    plt.bar(range(len(docs)),nums,width=0.4)

    plt.xticks(range(len(docs)),docs)

    plt.xlabel('doctype')

    plt.ylabel('number of publications')

    plt.tight_layout()

    plt.savefig('fig/doctype_num.png',dpi=200)

    logging.info('fig saved to fig/doctype_num.png')

    open('data/doctype_num.json','w').write(json.dumps(doctype_num))

    logging.info('Total number of papers:{}.'.format(np.sum(year_num.values())))


if __name__ == '__main__':
    # 获取论文的发布时间及文档类型
    process_pid_year_doctype()


