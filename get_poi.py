import overpy
import geopandas as gpd
from shapely.geometry import Point
import time
import contextily as ctx
import pandas as pd
import matplotlib.pyplot as plt
import random

# 创建 Overpass API 对象
api = overpy.Overpass()


# 定义查询语句（获取所有POI数据）
def fetch_poi_data(bbox):
    query = f"""
    [out:json][timeout:120];
    (
      node{bbox};
    );
    out body;
    >;
    out skel qt;
    """
    return query


# 添加请求间隔
REQUEST_INTERVAL = 5  # 设置每次请求的间隔时间（单位：秒）
# 设置最大数据条数限制
MAX_POI_COUNT = 600000


# 生成经纬度网格
def generate_bbox(lat_min, lon_min, lat_max, lon_max, step=0.2):
    bboxes = []
    for lat in range(int((lat_max - lat_min) / step)):
        for lon in range(int((lon_max - lon_min) / step)):
            bboxes.append(
                (lat_min + lat * step, lon_min + lon * step, lat_min + (lat + 1) * step, lon_min + (lon + 1) * step))
    return bboxes


# 保存数据到 CSV
def save_poi_data(poi_data, filename="poi_data_points.csv"):
    if poi_data:
        gdf_poi = gpd.GeoDataFrame(poi_data, geometry="geometry", crs="EPSG:4326")
        gdf_poi.to_csv(filename, index=False)
        print(f"Data saved to {filename}")


# 处理POI数据并增加进度反馈和定期保存
def fetch_pois_with_progress(bboxes, filename="poi_data_points.csv"):
    poi_data = []
    total_bboxes = len(bboxes)
    for idx, bbox in enumerate(bboxes):
        try:
            # 查询当前bbox的数据
            result = api.query(fetch_poi_data(bbox))
            for node in result.nodes:
                amenity = node.tags.get("amenity", "")
                if amenity:  # 仅保留amenity标签非空的数据
                    poi_data.append({
                        "id": node.id,
                        "amenity": amenity,
                        "name": node.tags.get("name", ""),
                        "latitude": node.lat,
                        "longitude": node.lon,
                        "geometry": Point(node.lon, node.lat),
                        "phone": node.tags.get("phone", "")
                    })

            # 打印当前进度
            print(f"Processed {idx + 1}/{total_bboxes} BBoxes, POIs fetched: {len(poi_data)}")

            # 每处理完一定数量的bbox就保存一次
            if (idx + 1) % 10 == 0:  # 每10个bbox保存一次数据
                save_poi_data(poi_data, filename)

            # 如果数据数量超过最大条数，停止爬取
            if len(poi_data) >= MAX_POI_COUNT:
                print(f"Maximum number of POIs reached: {MAX_POI_COUNT}. Stopping the program.")
                break


            # 等待一段时间避免频繁请求s
            time.sleep(random.randint(2,8))

        except Exception as e:
            print(f"Failed to fetch data for bbox {bbox}: {e}")
            continue  # 即使出错也跳过该区域继续处理其他区域

    # 最终保存所有数据
    print(f"Current number of POIs: {len(poi_data)}")
    save_poi_data(poi_data, filename)


# 确保经纬度的顺序是正确的：lat_min <= lat_max 和 lon_min <= lon_max
# lat_min, lat_max, lon_min, lon_max = 40.4774, 40.9176, -74.2591, -73.7004 # NewYork
# lat_min, lat_max, lon_min, lon_max = 36.0754, 45.3196, -79.8461, -68.1134 # 10NewYork
# lat_min, lat_max, lon_min, lon_max = 42.6754, 45.3196, -79.8461, -68.1134 # 10NewYork补充
# lat_min, lat_max, lon_min, lon_max = 34.4776, 43.7218, -100.4450, -88.7122  # kansasCity
# lat_min, lat_max, lon_min, lon_max = 37.2776, 43.7218, -100.4450, -88.7122  # kansasCity补Kc
lat_min, lat_max, lon_min, lon_max = 42.6754, 45.3196, -88.7122, -79.8461 # 10NewYorkLeft

# lat_min, lat_max, lon_min, lon_max = 34.4776, 43.7218, -113.4450, -101.7122  # kansasCity左
# lat_min, lat_max, lon_min, lon_max = 14.8105, 24.0547, -104.9996, -93.2668  # mexico
# lat_min, lat_max, lon_min, lon_max = 24.396308, 49.384358, -125.000000, -66.934570 # USA
# lat_min, lat_max, lon_min, lon_max = 40.4774, 40.6775, -74.2591, -74.1590

lost_bboxes = [(42.4754, -71.2461, 42.6754, -71.0461),(42.4754, -71.0461, 42.6754, -70.8461),(42.4754, -70.8461, 42.6754, -70.6461),(42.4754, -70.6461, 42.6754, -70.4461),
               (42.4754, -70.4461, 42.6754, -70.2461),(42.4754, -70.2461, 42.6754, -70.0461),(42.4754, -70.0461, 42.6754, -69.8461),
               (42.4754, -69.8461, 42.6754, -69.6461),(42.4754, -69.6461, 42.6754, -69.4461),(42.4754, -69.4461, 42.6754, -69.2461),
               (42.4754, -69.2461, 42.6754, -69.0461),(42.4754, -69.0461, 42.6754, -68.8461),(42.4754, -68.8461, 42.6754, -68.6461),
               (42.4754, -68.6461, 42.6754, -68.4461),(42.4754, -68.4461, 42.6754, -68.2461)]

# 获取网格区域（每个网格为1度的经纬度范围）
bboxes = generate_bbox(lat_min, lon_min, lat_max, lon_max, step=0.2)
# bboxes = lost_bboxes+bboxes

# 运行爬取函数
fetch_pois_with_progress(bboxes, "poi_data_points_nk.csv")

# 绘制 POI 分布图
poi_data_df = pd.read_csv("poi_data_points_nk.csv")
gdf_poi = gpd.GeoDataFrame(poi_data_df, geometry=gpd.GeoSeries.from_wkt(poi_data_df['geometry']), crs="EPSG:4326")

fig, ax = plt.subplots(figsize=(20, 12))
gdf_poi = gdf_poi.to_crs(epsg=3857)  # 转换为 Web Mercator 投影
gdf_poi.plot(ax=ax, color="blue", alpha=0.7, markersize=10, label="POIs")
ctx.add_basemap(ax, crs=gdf_poi.crs.to_string(), source=ctx.providers.OpenStreetMap.Mapnik)

plt.title("POI Distribution with OpenStreetMap Background", fontsize=14)
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.legend()
plt.grid(True)
plt.show()
