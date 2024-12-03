import redis
import csv
import math
import argparse
import os
import time

def clear_database():
    """
    清空 Redis 数据库中的所有数据
    """
    r.flushdb()

def add_poi(poi_id, amenity, name, latitude, longitude, geometry, phone):
    """
    添加一个 POI 到 Redis 数据库
    """
    # 使用哈希存储 POI 详细信息
    r.hset(f"poi:{poi_id}", mapping={
        "id": poi_id,
        "amenity": amenity,
        "name": name,
        "latitude": latitude,
        "longitude": longitude,
        "geometry": geometry,
        "phone": phone
    })
    # 将 POI 添加到对应名称和设施类型的集合中
    r.sadd(f"amenity:{amenity}", poi_id)
    r.sadd(f"name:{name}", poi_id)
    r.sadd(f"latitude:{latitude}", poi_id)
    r.sadd(f"longitude:{longitude}", poi_id)
    r.sadd(f"phone:{phone}", poi_id) 
    # 使用 GEOADD 存储地理位置
    r.geoadd("pois", (longitude, latitude, poi_id))

def store_pois_from_csv(file_path):
    """
    从 CSV 文件中加载 POI 数据并存储到 Redis 数据库
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            poi_id = row['id']
            if not poi_id.isdigit():
                print(f"跳过非数字 POI ID: {poi_id}")
                continue
            add_poi(poi_id, row['amenity'], row['name'], row['latitude'], row['longitude'], row['geometry'], row['phone'])
        print(f"已存储{file_path}中的POI数据!")

def get_pois_within_bbox(top_left_lat, top_left_lon, bottom_right_lat, bottom_right_lon):
    """
    获取指定矩形区域内的所有 POI
    """
    # 确定最小和最大纬度、经度
    min_lat = min(top_left_lat, bottom_right_lat)
    max_lat = max(top_left_lat, bottom_right_lat)
    min_lon = min(top_left_lon, bottom_right_lon)
    max_lon = max(top_left_lon, bottom_right_lon)

    # 计算中心点
    center_lat = (min_lat + max_lat) / 2
    center_lon = (min_lon + max_lon) / 2

    # 计算宽度和高度（以公里为单位）
    # 每一纬度约等于111公里
    lat_km = 111
    # 每一经度在不同纬度下长度不同，这里近似计算
    lon_km = 111 * abs(math.cos(math.radians(center_lat)))

    width_km = abs(max_lon - min_lon) * lon_km
    height_km = abs(max_lat - min_lat) * lat_km

    # 计算对角线半径
    radius = math.hypot(width_km / 2, height_km / 2)

    # 使用 GEORADIUS 获取初步结果
    poi_items = r.georadius("pois", center_lon, center_lat, radius, unit='km', withcoord=True)

    # 过滤出矩形范围内的 POI
    pois_id = set()
    for item in poi_items:
        poi_id = item[0]
        lon, lat = item[1]
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            pois_id.add(poi_id)
    return pois_id

def search_pois(poi_id=None, amenity=None, name=None, latitude=None, longitude=None, radius=None, phone=None, bbox=None):
    """
    综合查询
    returns: 查询结果POI list, log, 搜索条数
    """
    sets = []
    cod = 0
    log = ""
    lens = 0

    # 属性查询
    if poi_id:
        if r.exists(f"poi:{poi_id}"):
            sets.append(set([f"poi:{poi_id}".encode('utf-8')]))
            cod += 1
            lens += 1
            log += f'使用条件:id为 {poi_id}查询到1条POI数据。\n'
        else:
            return [], f"POI ID {poi_id} 不存在"
    if amenity:
        amenity_set = r.smembers(f"amenity:{amenity}")
        if amenity_set:
            sets.append(amenity_set)
            cod += 1
            lens += len(amenity_set)
            log += f'条件{cod}:设施类型为 {amenity},查询到{len(amenity_set)}条POI数据。\n'
        else:
            return [], f"没有找到 amenity 为 '{amenity}' 的 POI"
    if name:
        name_set = r.smembers(f"name:{name}")
        if name_set:
            sets.append(name_set)
            cod += 1
            lens += len(name_set)
            log += f'条件{cod}:设施名称为 {name},查询到{len(name_set)}条POI数据。\n'
        else:
            return [], f"没有找到名称为 '{name}' 的 POI"
    
    if latitude is not None and longitude is not None and radius is not None:
        # 查询给定半径范围内的所有 POI
        geo_poi_ids = r.georadius("pois", longitude, latitude, radius, unit='km')
        geo_poi_set = set(geo_poi_ids)
        if geo_poi_set:
            sets.append(set(geo_poi_set))
            cod += 1
            lens += len(geo_poi_set)
            log += f'条件{cod}:在({longitude},{latitude})的半径 {radius} km内查询到{len(geo_poi_set)}条POI数据。\n'
        else:
            return [], f"在({longitude},{latitude})的半径 {radius} km内没有找到 POI"
    elif latitude is not None and longitude is None:
        # 查询给定纬度上的所有 POI
        poi_ids = r.keys("poi:*")
        lat_poi_set = set()
        for poi_id in poi_ids:
            poi = r.hgetall(poi_id)
            if poi and float(poi[b'latitude']) == latitude:
                lat_poi_set.add(poi_id)
        if lat_poi_set:
            sets.append(lat_poi_set)
            cod += 1
            lens += len(lat_poi_set)
            log += f'条件{cod}:纬度为 {latitude} ,查询到{len(lat_poi_set)}条POI数据。\n'
        else:
            return [], f"没有找到纬度为 {latitude} 的 POI"
    elif latitude is None and longitude is not None:
        # 查询给定经度上的所有 POI
        poi_ids = r.keys("poi:*")
        lon_poi_set = set()
        for poi_id in poi_ids:
            poi = r.hgetall(poi_id)
            if poi and float(poi[b'longitude']) == longitude:
                lon_poi_set.add(poi_id)
        if lon_poi_set:
            sets.append(lon_poi_set)
            cod += 1
            lens += len(lon_poi_set)
            log += f'条件{cod}:经度为 {longitude} ,查询到{len(lon_poi_set)}条POI数据。\n'
        else:
            return [], f"没有找到经度为 {longitude} 的 POI"
        
    # 电话号查询
    if phone:
        phone_set = r.smembers(f"phone:{phone}")
        if phone_set:
            sets.append(phone_set)
            cod += 1
            lens += len(phone_set)
            log += f'条件{cod}:电话号为 {phone},查询到{len(phone_set)}条POI数据。\n'
        else:
            return [], f"没有找到电话号为 '{phone}' 的 POI"
        
    # 包围框查询
    if bbox:
        bbox = bbox.split(",")
        assert len(bbox) == 4, "包围框参数必须包含4个值"
        top_left_lat, top_left_lon, bottom_right_lat, bottom_right_lon = bbox
        pois_id = get_pois_within_bbox(float(top_left_lat), float(top_left_lon), float(bottom_right_lat), float(bottom_right_lon))
        if pois_id:
            sets.append(pois_id)
            cod += 1
            lens += len(pois_id)
            log += f'条件{cod}:包围框为{bbox},查询到{len(pois_id)}条POI数据。\n'
        else:
            return [], f"包围框 {bbox} 内没有找到 POI"
    
    if not sets:
        return [], "没有提供任何查询条件"

    # 取所有条件的交集
    result_ids = set.intersection(*sets)
    # 获取 POI 详细信息
    if len(result_ids)==1:
        pois = [r.hgetall(poi_id if poi_id.startswith(b"poi:") else f"poi:{poi_id.decode('utf-8')}".encode('utf-8')) for poi_id in result_ids]
    else:
        pois = [r.hgetall(f"poi:{poi_id.decode('utf-8')}") for poi_id in result_ids]
    return pois, log + f"取交集共查询到{len(pois)}条POI数据!" , lens

def save_output(pois, path, log):
    """存储查询结果"""
    # 定义 CSV 文件的列名
    fieldnames = ["id", "amenity", "name", "latitude", "longitude", "geometry", "phone"]
    os.makedirs(path, exist_ok=True)
    
    csv_path = os.path.join(path, "pois.csv")
    log_path = os.path.join(path, "log.txt")
    
    # 如果文件已存在，先删除
    if os.path.exists(csv_path):
        os.remove(csv_path)
    if os.path.exists(log_path):
        os.remove(log_path)
    
    # 打开文件进行写操作
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for poi in pois:
            decoded_poi = {key.decode('utf-8'): value.decode('utf-8') for key, value in poi.items()}
            writer.writerow(decoded_poi)
    
    with open(log_path, mode='w', encoding='utf-8') as file:
        file.write(log)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db", type=int, default=0, help="Redis数据库编号,0为所有数据,1-5分别为10w-50w数据"
    )
    parser.add_argument(
        "--clear", action="store_true", help="清空Redis数据库中的所有数据"
    )
    parser.add_argument(
        "--store", type=str, help="从输入路径的CSV文件中存储POI数据"
    )
    parser.add_argument(
        "--output", type=str, help="将查询结果储存到该路径"
    )
    parser.add_argument("--id", type=str, help="提供索引id")
    parser.add_argument("--amenity", type=str, help="提供索引设施类型")
    parser.add_argument("--name", type=str, help="提供索引设施名称")
    parser.add_argument("--longitude", type=float, help="提供索引经度")
    parser.add_argument("--latitude", type=float, help="提供索引纬度")
    parser.add_argument("--phone", type=str, help="提供索引电话号")
    parser.add_argument("--bounding-box", type=str, help="提供索引包围框")
    parser.add_argument("--radius", type=float, help="提供索引半径,单位为km")
    args = parser.parse_args()

    r = redis.Redis(host='localhost', port=6379, db=args.db)
    
    if args.clear:
        clear_database()

    if args.store:
        assert os.path.exists(args.store), f"不存在输入文件{args.store}"
        store_pois_from_csv(args.store)
    
    if args.output:
        if args.id:
            assert not (args.amenity or args.name or args.longitude or args.latitude or args.phone), "若提供id,其他索引无效!"
            start_time = time.time()
            pois, log, lens = search_pois(poi_id=args.id)      
        else:
            if args.bounding_box:
                assert not (args.latitude or args.longitude), "若提供包围框,经纬度无效!"
            if args.latitude and args.longitude:
                assert args.radius, "若提供经纬度,必须提供半径!"
            elif args.latitude:
                assert not args.radius, "若仅提供纬度,半径无效!"
            else:
                assert not args.radius, "若仅提供经度,半径无效!"
            start_time = time.time()
            pois, log, lens= search_pois(amenity=args.amenity, name=args.name, latitude=args.latitude, longitude=args.longitude, radius=args.radius, phone=args.phone, bbox=args.bounding_box)
        end_time = time.time()
        delta_time = end_time - start_time
        throughputs = lens/delta_time
        log = log+f"\n搜索时间为{delta_time}秒,吞吐量为{throughputs}条/秒!"
        save_output(pois, args.output, log)

    print("已完成!")

