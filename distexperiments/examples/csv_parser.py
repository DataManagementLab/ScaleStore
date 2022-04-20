import config
from distexprunner import *


server_list = config.server_list[0, ]


@reg_exp(servers=server_list)
def csv_parser(servers):
    s = servers[0]

    csvs = IterClassGen(CSVGenerator,
        r'value=(?P<value>\d+)',    # catches only latest printed value
        CSVGenerator.Array(r'other=(?P<other>\d+)'), # collects all in a '|' separated array
        CSVGenerator.Sum(r'other=(?P<other_sum>\d+)'),
        CSVGenerator.SortedArray(r'value=(?P<i>\d+),other=(?P<sorted_other>\d+)'),
        foobar=1234,
    )
    s.run_cmd('for i in {10..1}; do echo "value=$i,other=$((i*2))"; done', stdout=next(csvs)).wait()

    for csv in csvs:
        # writes header once and appends rows
        # csv has properties .header and .row
        csv.write('file.csv')

    # file.csv:
    # value,other,other_sum,sorted_other
    # 1,20|18|16|14|12|10|8|6|4|2,110,2|4|6|8|10|12|14|16|18|20


