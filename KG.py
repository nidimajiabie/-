from py2neo import Graph, Node, Relationship, NodeMatcher, Subgraph
from py2neo.matching import RelationshipMatcher
import pandas as pd


# 建立一个节点
def create_node(graph, label, key):
    '''
    生成一个标签为label,标签值为key的节点
    :param graph: 用来连接neo4j
    :param label: 字符串
    :param key: 字符串，非字符串也行，本函数会将key转化为字符串
    :return: 返回一个节点
    '''
    matcher = NodeMatcher(graph)
    # 查询是否已经存在，若存在则返回节点，否则返回None
    value = matcher.match(label).where(name = '%s'% key).first()
    # 如果要创建的节点不存在则创建
    if value is None:
        node = Node(label, name = '%s'% key)
        graph.create(node)
        return node
    else:
        return None

# 给节点添加属性
def add_attr(graph, node, attr_name, attr_key):
    '''
    给某个节点增加属性
    :param graph: 用来连接neo4j
    :param node: 节点label
    :param attr_name: 要添加的属性名字
    :param attr_key: 要添加的属性值
    :return: 返回添加了属性的新节点，节点label保持不变
    '''
    node['%s' % attr_name] = '%s' % attr_key
    graph.push(node)
    return node

# 查询节点
def match_node(graph, label, key):
    '''
    查询一个标签为label，标签值为key的节点
    :param graph: 用来连接neo4j
    :param label: 字符串
    :param key: 字符串，非字符串也行，本函数会将key转化为字符串
    :return: 返回要查询的节点，如果查询的节点不存在，则返回None
    '''
    matcher = NodeMatcher(graph)
    return matcher.match(label).where(name = '%s'% key).first()

# 建立两个节点之间的关系
def create_relationship(graph, label1, key1, label2, key2, r_name):
    '''
    建立两个节点的关系，方向从下标1的节点到下标2的节点
    :param graph: 用来连接neo4j
    :param label1: 起始节点的标签，字符串
    :param key1: 起始节点的标签值，字符串，非字符串也行，本函数会将key1转化为字符串
    :param label2: 结束节点的标签，字符串
    :param key2: 结束节点的标签值，字符串，非字符串也行，本函数会将key2转化为字符串
    :param r_name: 两个节点的关系名字
    :return: 返回两个节点的关系
    '''
    value1 = match_node(graph, label1, key1)
    value2 = match_node(graph, label2, key2)
    if value1 is None or value2 is None:
        return False
    r = Relationship(value1, r_name, value2)
    graph.create(r)
    return r

# 删除一个节点及与之相连的关系
def delete_node(graph, label, key):
    '''
    删除一个连点及与之相连的关系
    :param graph: 用来连接neo4j
    :param label: 字符串
    :param key: 字符串，非字符串也行，本函数会将key1转化为字符串
    :return: 删除目标节点及及与之相连的关系
    '''
    graph.run("match (n:%s {name: '%s'}) detach delete n"%(label, key))

# 创建两个节点和一个关系，形成一条路径
def createpath(graph, label1, key1, label2, key2, r_name=None):
    '''
    创建两个节点和两个节点的关系，即一条路径
    :param graph: 用来连接neo4j
    :param label1: 起始节点的标签，字符串
    :param key1: 起始节点的标签值，字符串，非字符串也行，本函数会将key1转化为字符串
    :param label2: 结束节点的标签，字符串
    :param key2: 结束节点的标签值，字符串，非字符串也行，本函数会将key2转化为字符串
    :param r_name: 两个节点的关系名字
    :return:返回两个相邻节点构成的路径
    '''
    node1 = create_node(graph, label1, key1)
    node2 = create_node(graph, label2, key2)
    r = create_relationship(graph, label1, key1, label2, key2, r_name)
    return node1,node2,r

# 查询关系
def search_relationship(graph, node1=None, node2=None, relation_type = None):
    '''
    依据关系类型查询起始的节点node1和结束的节点node2的关系，注意只是两个相邻节点的关系。x/y/relation_type默认值为None，表示不指定
    :param graph: 用来连接neo4j
    :param node1: 起始的节点
    :param node2: 结束的节点
    :param relation_type: 关系类型，字符串
    :return: 返回满足条件的关系
    '''
    relationship_matcher = RelationshipMatcher(graph)
    relationship = list(relationship_matcher.match((node1, node2), r_type= relation_type))
    return relationship

# 查询路径
def search_path(graph, entity1, entity2, entity_name1, entity_name2):
    '''
    依据起始节点的标签、标签值和结束节点的标签、标签值，寻找起始节点到结束节点的路径
    :param graph: 用来连接neo4j
    :param entity1: 起始节点的标签，字符串
    :param entity2: 结束节点的标签，字符串
    :param entity_name1: 起始节点的标签值，字符串型
    :param entity_name2: 结束节点的标签值，字符串型
    :return: 起始节点到结束节点的路径，放在s中，s为series(也可以用其他格式储存，例如dataframe)
    '''
    cypher_ = "match path=(n:%s)-[*]->(m:%s) where n.name='%s' and m.name='%s' RETURN path"%(entity1, entity2, entity_name1, entity_name2)
    s = graph.run(cypher_).to_series()
    return s

# 查询完整路径
def search_path_complete(graph, x, ylabel):
    '''
    依据24个要素x，查询其对应的y，并输出x->y的完整路径
    :param graph: 用来连接neo4j
    :param x: 一组至少涵盖24个要素的列表
    :param ylabel: 响应变量的label
    :return: 返回x->y的完整路径
    '''
    xlabel = ['年龄类名', '职业类型', '危险驾驶罪前科类名', '酒后驾驶被行政处罚劣迹类名', '其他犯罪前科类名',
              '其他行政处罚类名', '超过80后增加了几个30类名', '车辆类名', '负事故责任类型', '是否单车事故', '造成他人受伤类名',
              '造成他人经济损失类名', '道路类型', '违反道交法类型', '逃避抗检类型', '毒驾类名', '非常规道路类名',
              '赔偿谅解情况类名', '是否紧急避险', '行为能力类名', '自首类名', '坦白类名', '是否认罪认罚', '立功类名']
    xname = [x[i] for i in xlabel]
    yname = x[ylabel]
    cypher_ = "MATCH path=(xl1:%s)-[]->(xl2:%s)-[]->(xl3:%s)-[]->(xl4:%s)-[]->(xl5:%s)-[]->" \
              "(xl6:%s)-[]->(xl7:%s)-[]->(xl8:%s)-[]->(xl9:%s)-[]->(xl10:%s)-[]->(xl11:%s)-[]->" \
              "(xl12:%s)-[]->(xl13:%s)-[]->(xl14:%s)-[]->(xl15:%s)-[]->(xl16:%s)-[]->(xl17:%s)-[]->" \
              "(xl18:%s)-[]->(xl19:%s)-[]->(xl20:%s)-[]->(xl21:%s)-[]->(xl22:%s)-[]->(xl23:%s)-[]->" \
              "(xl24:%s)-[]->(y:%s) " \
              "WHERE xl1.name='%s' and xl2.name='%s' and xl3.name='%s' and xl4.name='%s' and " \
              "xl5.name='%s' and xl6.name='%s' and xl7.name='%s' and xl8.name='%s' and xl9.name='%s' and " \
              "xl10.name='%s' and xl11.name='%s' and xl12.name='%s' and xl13.name='%s' and xl14.name='%s' and " \
              "xl15.name='%s' and xl16.name='%s' and xl17.name='%s' and xl18.name='%s' and xl19.name='%s' and " \
              "xl20.name='%s' and xl21.name='%s' and xl22.name='%s' and xl23.name='%s' and xl24.name='%s'" \
              "and y.name='%s' RETURN path" %(tuple(xlabel+[ylabel]+xname+[yname]))
    path = graph.run(cypher_).to_series()
    return path

# 查询节点
def search_node(graph, entity1, entity2, entity_name1):
    '''
    依据起始节点的标签、标签值和结束节点的标签，寻找结束节点的标签值
    :param graph: 用来连接neo4j
    :param entity1: 起始节点的标签，字符串
    :param entity2: 结束节点的标签，字符串
    :param entity_name1: 起始节点的标签值，字符串型
    :return: 结束节点的标签值
    '''
    cypher_ = "MATCH path=(n:%s)-[*]->(m:%s) WHERE n.name='%s' RETURN m.name"%(entity1, entity2, entity_name1)
    entity_name2 = graph.run(cypher_).to_series()
    return entity_name2

def rewrite_path(s, relation = False):
    '''
    把py2neo输出的路径重新排版
    :param s: 路径,a series
    :return: 重新排版的路径
    '''
    path_texts = []
    for path in s:
        # 获取路径中的节点和关系
        nodes = path.nodes
        relationshis = path.relationships
        # 自己组织路径文本
        path_text = ""
        for n, r in zip(nodes, relationshis):
            # 每次加入一个节点和一个关系的类型
            path_text += "{} - {} - ".format(n['name'], type(r).__name__)
        # 别忘了最后一个节点
        path_text += nodes[-1]['name']
        path_texts.append(path_text)
    if relation == False:
        node_texts = []
        for path_text in path_texts:
            path_text = path_text.split(' - ')
            path_text = path_text[::2]
            node_texts.append(path_text)
        return node_texts
    else:
        path_texts = path_texts
        return path_texts

def create_KG(graph, data):
    '''
    架构知识图谱
    :param graph: 用来连接neo4j
    :param data_train: a dataframe, 架构图谱用到的数据
    :return:
    '''
    ### 架构知识图谱
    ## 建立第一层节点,第一层节点不融合
    for i in range(len(data)):
        # 创建节点和关系
        start_node = Node('判决书号', name='%s' % data['判决书号'][i])
        end_node = Node('年龄类名', name='%s' % data['年龄类名'][i])
        relation_name = '从轻要素'
        relation = Relationship(start_node, relation_name, end_node)

        # 给节点增加属性
        add_attr(graph, start_node, '所在区', data['所在区'][i])
        add_attr(graph, start_node, '区号', data['区号'][i])
        add_attr(graph, start_node, '姓名', data['姓名'][i])
        add_attr(graph, start_node, '性别', data['性别'][i])
        add_attr(graph, start_node, '民族', data['民族'][i])
        add_attr(graph, start_node, '学历', data['学历'][i])
        add_attr(graph, start_node, '户籍地', data['户籍地'][i])
        add_attr(graph, start_node, '户籍地所在省', data['户籍地所在省'][i])
        add_attr(graph, start_node, '户籍地所在市', data['户籍地所在市'][i])
        add_attr(graph, start_node, '居住地', data['居住地'][i])
        add_attr(graph, start_node, '居住地所在市', data['居住地所在市'][i])

        # 节点融合,避免重复节点 注意第一层节点不融合
        graph.merge(end_node, '年龄类名', "name")

        # 创建节点和关系
        graph.create(start_node)
        graph.create(end_node)
        graph.create(relation)
    ## 架构关系
    # 前科劣迹
    data.apply(lambda x: createpath(graph, '年龄类名', x['年龄类名'], '职业类型', x['职业类型'], '从轻要素'), axis=1)
    data.apply(lambda x: createpath(graph, '职业类型', x['职业类型'], '危险驾驶罪前科类名', x['危险驾驶罪前科类名'], '入罪要素'), axis=1)
    data.apply(
        lambda x: createpath(graph, '危险驾驶罪前科类名', x['危险驾驶罪前科类名'], '酒后驾驶被行政处罚劣迹类名', x['酒后驾驶被行政处罚劣迹类名'], '入罪要素'), axis=1)
    data.apply(
        lambda x: createpath(graph, '酒后驾驶被行政处罚劣迹类名', x['酒后驾驶被行政处罚劣迹类名'], '其他犯罪前科类名', x['其他犯罪前科类名'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '其他犯罪前科类名', x['其他犯罪前科类名'], '其他行政处罚类名', x['其他行政处罚类名'], '入罪要素'), axis=1)
    # 驾驶行为
    data.apply(
        lambda x: createpath(graph, '其他行政处罚类名', x['其他行政处罚类名'], '超过80后增加了几个30类名', x['超过80后增加了几个30类名'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '超过80后增加了几个30类名', x['超过80后增加了几个30类名'], '车辆类名', x['车辆类名'], '入罪要素'),
                     axis=1)
    data.apply(lambda x: createpath(graph, '车辆类名', x['车辆类名'], '负事故责任类型', x['负事故责任类型'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '负事故责任类型', x['负事故责任类型'], '是否单车事故', x['是否单车事故'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '是否单车事故', x['是否单车事故'], '造成他人受伤类名', x['造成他人受伤类名'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '造成他人受伤类名', x['造成他人受伤类名'], '造成他人经济损失类名', x['造成他人经济损失类名'], '入罪要素'),
                     axis=1)
    data.apply(lambda x: createpath(graph, '造成他人经济损失类名', x['造成他人经济损失类名'], '道路类型', x['道路类型'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '道路类型', x['道路类型'], '违反道交法类型', x['违反道交法类型'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '违反道交法类型', x['违反道交法类型'], '逃避抗检类型', x['逃避抗检类型'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '逃避抗检类型', x['逃避抗检类型'], '毒驾类名', x['毒驾类名'], '入罪要素'), axis=1)
    data.apply(lambda x: createpath(graph, '毒驾类名', x['毒驾类名'], '非常规道路类名', x['非常规道路类名'], '入罪要素'), axis=1)
    # 从轻要素
    data.apply(lambda x: createpath(graph, '非常规道路类名', x['非常规道路类名'], '赔偿谅解情况类名', x['赔偿谅解情况类名'], '从轻要素'), axis=1)
    data.apply(lambda x: createpath(graph, '赔偿谅解情况类名', x['赔偿谅解情况类名'], '是否紧急避险', x['是否紧急避险'], '从轻要素'), axis=1)
    data.apply(lambda x: createpath(graph, '是否紧急避险', x['是否紧急避险'], '行为能力类名', x['行为能力类名'], '从轻要素'), axis=1)
    data.apply(lambda x: createpath(graph, '行为能力类名', x['行为能力类名'], '自首类名', x['自首类名'], '从轻要素'), axis=1)
    data.apply(lambda x: createpath(graph, '自首类名', x['自首类名'], '坦白类名', x['坦白类名'], '从轻要素'), axis=1)
    data.apply(lambda x: createpath(graph, '坦白类名', x['坦白类名'], '是否认罪认罚', x['是否认罪认罚'], '从轻要素'), axis=1)
    data.apply(lambda x: createpath(graph, '是否认罪认罚', x['是否认罪认罚'], '立功类名', x['立功类名'], '从轻要素'), axis=1)
    # 判罚结果
    data.apply(lambda x: createpath(graph, '立功类名', x['立功类名'], '拘役时长分段', x['拘役时长分段'], '判罚结果'), axis=1)
    data.apply(lambda x: createpath(graph, '立功类名', x['立功类名'], '罚金分段', x['罚金分段'], '判罚结果'), axis=1)
    data.apply(lambda x: createpath(graph, '立功类名', x['立功类名'], '是否缓刑', x['是否缓刑'], '判罚结果'), axis=1)

def renew_pathsdata(graph, data, savepath1, savepath2, savepath3):
    '''
    将新的拘役时长的路径添加到拘役时长的数据库中；将新的罚金的路径添加到罚金的数据库中；将新的缓刑的路径添加到缓刑的数据库中
    :param graph: 用来连接neo4j
    :param data: a dataframe
    :param savepath1: 用来储存拘役时长的路径
    :param savepath2: 用来储存罚金的路径
    :param savepath3: 用来储存缓刑的路径
    :return:
    '''
    fine_paths = []
    detention_paths = []
    probation_paths = []
    for i in range(len(data)):
        detention_path = rewrite_path(search_path_complete(graph, data.iloc[i], '拘役时长分段'))
        detention_paths = detention_paths + detention_path

        fine_path = rewrite_path(search_path_complete(graph, data.iloc[i], '罚金分段'))
        fine_paths = fine_paths + fine_path

        probation_path = rewrite_path(search_path_complete(graph, data.iloc[i], '是否缓刑'))
        probation_paths = probation_paths + probation_path
    with open(savepath1, 'a', encoding='utf-8') as f:
        for line in detention_paths:
            print(line, file=f)
    with open(savepath2, 'a', encoding='utf-8') as f:
        for line in fine_paths:
            print(line, file=f)
    with open(savepath3, 'a', encoding='utf-8') as f:
        for line in probation_paths:
            print(line, file=f)

# savepath1 = r'detentionpath.txt'
# savepath2 = r'finepath.txt'
# savepath3 = r'probationpath.txt'
# renew_pathsdata(graph, data, savepath1, savepath2, savepath3)

# 已知x，统计y各个组别的频数，并选择频数最大的组别作为y的取值
def y_max(x, paths):
    '''
    输入要素x和paths， 统计y各个组别的频数，并选择频数最大的组别作为y的取值
    :param x: a list, not a datadrame or a series
    :param paths: a list of lists, paths列表里面的每一个子列表包括要素x和响应变量y，如罚金/拘役时长/缓刑，其中y在最后一列
    :param ylabel: 字符串
    :return: 返回y频数最大对应的组别，同时返回y频数的分布
    '''
    # 筛选出和x要素一样的路径
    x = [str(i) for i in x]# 将x的元素转化为字符串型
    paths_candi = []
    ys_candi = []
    for path in paths:
        if path[0:-1] == x:
            paths_candi.append(path)
            y_candi = path[-1]
            ys_candi.append(y_candi)
        else:
            paths_candi = paths_candi
            ys_candi = ys_candi
    # 依据筛选后的路径，统计y组别的词频
    # print(pd.value_counts(ys_candi))
    if pd.value_counts(ys_candi).to_list() == []:
        print("there is no response in the historic data!!!")
        ymax = None
    else:
        countmax = max(pd.value_counts(ys_candi).to_list())
        ymax = list(dict(pd.value_counts(ys_candi)).keys())[list(dict(pd.value_counts(ys_candi)).values()).index(countmax)]
    return ymax, pd.value_counts(ys_candi)

def accuracy(y,yhat):
    '''
    计算预测的准确率
    :param y: a list
    :param yhat: a list
    :return: 返回一列结果的预测正确率
    '''
    n = len(y)
    count = 0
    for i in range(n):
        if y[i] == yhat[i]:
            count = count + 1
        else:
            count = count
    accur = count/n
    return accur