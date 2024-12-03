# RedisPOI

RedisPOI能爬取指定区域的POI，并将其存储于Redis数据库中。RedisPOI还实现了基本的查询检索和性能计算功能。

## 安装

* 安装Redis数据库。请参考[Redis](https://redis.io/)网站。
* 在自己的环境中按[requirements.txt](requirements.txt)的要求安装所需库。这是一个示例：
```bash
conda create -name redis-poi python=3.13.0
conda activate redis-poi
git clone https://github.com/QuanzhuNiu/RedisPOI.git
cd RedisPOI
pip install -r requirements.txt
```

## 爬取POI

* RedisPOI默认爬取所有公共设施(amenity不为空)，将每条POI的id、name、amenity、latitude、longitude、geomitry、phone存储下来。
* 你可以自行设置需要爬取的区域和属性。
```bash
python get_poi.py
```

## 清空数据库

* 若要查看[main_redis.py](main_redis.py)的功能，请使用以下指令：
```bash
python main_redis.py -h
```
* Redis默认提供16个数据库，可通过更改--db参数(0-15)选择要连接的数据库。若不指定，默认为0。
* --clear参数会清空连接的数据库的所有内容。例如：
```bash
python main_redis.py --db 4 --clear
```

## 向数据库中存储POI数据

* --store参数用来指定输入数据的路径。例如：
```bash
python main_redis.py --db 1 --store data_pois/10w.csv
```

## 在数据库中查询POI数据

* 若将--output参数设为一个路径，我们认为用户将在数据库中进行查询。查询结果将储存于此。
* 我们提供了两类查询方式：范围和属性。其中，范围查询可以在“中心点经纬度+半径”和“包围框左上、右下点坐标”两种方式内任选其一，或二者都不。
* 在范围查询之外，其他属性也可以自由组合。
* 需要注意的是，如果同时给定经度和纬度，我们认为用户要进行“中心点经纬度+半径”查询，需提供半径参数。
* 以下是一个示例：
```bash
python main_redis.py --db 1 --output output_test --bounding-box 10,-60,20,-35
```
* 或者你可以使用bash脚本，可客制化地多次查询，例如我们提供的[示例](run_search.sh)：
```bash
source run_search.sh
```

## 计算检索性能

* RedisPOI每次检索都会计算检索时长和吞吐量（单位时间检索的POI条数），并将其保存在log中，共用户参考。
* 我们提供了[collect_data.py](collect_data.py)用于将你的实验结果的检索时长和吞吐量汇总起来，方便统计实验数据。
