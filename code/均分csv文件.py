import csv
import math
import os

def split_csv_with_header(input_file, output_prefix, n):
    """
    将输入的CSV文件平均分成n份，每个文件都包含标题行
    参数:
        input_file: 输入的CSV文件路径
        output_prefix: 输出文件的前缀
        n: 要分割成的份数
    """
    # 读取原始CSV文件
    with open(input_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # 读取标题行
        rows = list(reader)    # 读取剩余数据行
    
    # 计算每份的行数
    total_rows = len(rows)
    rows_per_file = math.ceil(total_rows / n)
    
    # 写入分割后的文件
    for i in range(n):
        start = i * rows_per_file
        end = min((i + 1) * rows_per_file, total_rows)
        
        output_file = f"{output_prefix}_{i+1}.csv"
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)       # 写入标题行
            writer.writerows(rows[start:end])  # 写入数据行
        
        print(f"已创建文件: {output_file} (包含 {end - start} 行数据)")

if __name__ == "__main__":
    # 获取用户输入
    n = int(input("请输入要分割的份数: "))
    input_file_input = input("输入的CSV文件路径:").replace("\"","")
    output_prefix = input("输出文件的前缀:")
    input_file = rf"{input_file_input}"
    
    # 检查文件是否存在
    if not os.path.exists(input_file):
        print("错误: 输入文件不存在!")
    else:
        split_csv_with_header(input_file, output_prefix, n)