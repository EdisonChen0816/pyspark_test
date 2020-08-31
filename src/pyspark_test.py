# encoding=utf-8
from pyspark import SparkContext, SparkFiles
from operator import add


def test1():
    sc = SparkContext('local', 'first app')
    log_file = '/home/Edison/project/chatbot/README.md'
    log_data = sc.textFile(log_file).cache()
    num_a = log_data.filter(lambda s : 'a' in s).count()
    num_b = log_data.filter(lambda s : 'b' in s).count()
    print('a: %s, b: %s' % (num_a, num_b))


def test2():
    sc = SparkContext('local', 'first app')
    words = sc.parallelize(['scala', 'java', 'python', 'spark',
                            'machine learning', 'deep learning',
                            'cnn', 'rnn', 'bert', 'lstm'])
    counts = words.count()
    print('-------counts : %i ---------' % counts)
    coll = words.collect()
    print('-------collent : %s-------------' % coll)
    words_filter = words.filter(lambda s : 'spark' in s).collect()
    print('-------words_filter: %s---------------' % words_filter)
    words_map = words.map(lambda x : (x, 1))
    mapping = words_map.collect()
    print('--------mapping: %s-------------------' % mapping)


def test3():
    sc = SparkContext('local', 'first app')
    nums = sc.parallelize([1, 2, 3, 4, 5])
    adding = nums.reduce(add)
    print('---------adding: %s-----------------' % adding)
    x = sc.parallelize([('spark', 1), ('hadoop', 4)])
    y = sc.parallelize([('spark', 2), ('hadoop', 10)])
    joined = x.join(y)
    print('---------joined: %s-----------------' % joined.collect())
    nums.cache()
    print('---------cache: %s------------------' % nums.is_cached)


def test4():
    sc = SparkContext('local', 'first app')
    words = sc.broadcast(['scala', 'java', 'php', 'python'])
    data = words.value
    elem = data[2]
    print('-----data : %s-------' % data)
    print('-----elem : %s-------' % elem)
    num = sc.accumulator(10)
    num += 10
    print('--------accumlator: %s--------' % num.value)


def test5():
    sc = SparkContext('local', 'first app')
    file = '/home/Edison/project/chatbot/README.md'
    sc.addFile(file)
    print('------file: %s---------' % SparkFiles.get('README.md'))


if __name__ == '__main__':
    test1()
    test2()
    test3()
    test4()
    test5()