import os
import csv
import re

def extract_log_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        # 使用正则表达式提取搜索时间和吞吐量
        search_time = re.search(r"搜索时间为([\d\.]+)秒", content).group(1)
        throughput = re.search(r"吞吐量为([\d\.]+)条/秒", content).group(1)
        return float(search_time), float(throughput)

def save_to_csv(data, csv_path, header):
    with open(csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(data)

def main():
    base_path = './output'  # 修改为你的文件夹路径
    sub_dirs = [f"output_exp_10w_{i}" for i in range(1, 9)] + \
               [f"output_exp_20w_{i}" for i in range(1, 9)] + \
               [f"output_exp_30w_{i}" for i in range(1, 9)] + \
               [f"output_exp_40w_{i}" for i in range(1, 9)] + \
               [f"output_exp_50w_{i}" for i in range(1, 9)]

    search_times = []
    throughputs = []

    for sub_dir in sub_dirs:
        log_file_path = os.path.join(base_path, sub_dir, 'log.txt')
        if os.path.exists(log_file_path):
            search_time, throughput = extract_log_data(log_file_path)
            search_times.append([sub_dir, search_time])
            throughputs.append([sub_dir, throughput])
        else:
            print(f"{log_file_path} 不存在")

    # 保存搜索时间到 CSV 文件
    save_to_csv(search_times, os.path.join(base_path, 'search_times.csv'), ['Folder', 'Search Time (seconds)'])
    # 保存吞吐量到 CSV 文件
    save_to_csv(throughputs, os.path.join(base_path, 'throughputs.csv'), ['Folder', 'Throughput (records/second)'])

if __name__ == "__main__":
    main()
