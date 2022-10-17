#!/usr/bin/bash

num_date=$1
start_date=$2
end_date=$3

function find_rand_date {
    n=$1
    start=$2
    end=$3
    # 计算从开始的日期到结束的日期间隔了几天
    length=$((($(date +%s -d "${end}") - $(date +%s -d "${start}"))/86400));
    dates=""
    while [ $n -gt 0 ];
    do
        # 生成和间隔天数数位相同的随机数
        # 如果间隔12天，生成0-11之间的随机数
        # 如果间隔120天，生成0-119之间的随机数
        num=$((RANDOM%$length))
        d=`date -d "$end -$num day" +%Y-%m-%d`
        # 在字符串结尾增加内容
        dates=${dates}" ${d}"
        # 循环指标 n-1
        let n=`expr $n - 1`
    done
    echo ${dates: 1}
}

dates=`find_rand_date $num_date $start_date $end_date`
echo $dates
for d in $dates
do
    echo "$d 数据开始跑数"
    sh run.sh --config ./config/config.toml --the_date $d --file_no all
    # $!是Shell最后运行的后台Process的PID
    if [[ $! -eq 0 ]]
    then
        echo "$d 数据跑数结束"
    else
        exit
    fi
done
