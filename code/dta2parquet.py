# 说明：
# 这是一个 将.dta文件转换为.parquet文件，支持分块读取大文件
# 参数:
#   input_folder (str): 输入文件夹路径
#   output_folder (str): 输出文件夹路径
#   chunksize (int): 分块大小（行数），默认10000行

import os
import pandas as pd
from tqdm import tqdm

def convert_dta_to_parquet(input_folder, output_folder, chunksize=10000):
    # 检查输入文件夹是否存在
    if not os.path.isdir(input_folder):
        print(f"错误: 输入文件夹 '{input_folder}' 不存在")
        return
    
    # 创建输出文件夹（如果不存在）
    os.makedirs(output_folder, exist_ok=True)
    
    # 获取所有.dta文件
    dta_files = [f for f in os.listdir(input_folder) 
                 if os.path.isfile(os.path.join(input_folder, f)) 
                 and f.lower().endswith('.dta')]
    
    if not dta_files:
        print(f"未找到.dta文件: {input_folder}")
        return
    
    print(f"找到 {len(dta_files)} 个.dta文件，开始转换...")
    
    # 遍历文件转换
    for filename in tqdm(dta_files, desc="总体进度"):
        try:
            dta_path = os.path.join(input_folder, filename)
            parquet_filename = os.path.splitext(filename)[0] + '.parquet'
            parquet_path = os.path.join(output_folder, parquet_filename)
            
            # 先尝试普通模式读取（小文件更快）
            try:
                tqdm.write(f"普通模式读取: {filename}")
                df = pd.read_stata(dta_path)
                df.to_parquet(parquet_path, index=False)
                
            except MemoryError:
                # 普通模式内存不足时，自动切换分块模式
                tqdm.write(f"内存不足，启用分块模式: {filename} (每次读取 {chunksize} 行)")
                first_chunk = True
                
                # 分块读取并追加写入
                for chunk in pd.read_stata(dta_path, chunksize=chunksize):
                    chunk.to_parquet(
                        parquet_path,
                        index=False,
                        mode='w' if first_chunk else 'a',  # 第一块创建，后续追加
                        engine='pyarrow'
                    )
                    first_chunk = False
            
            # 验证转换结果
            if os.path.exists(parquet_path):
                tqdm.write(f"✅ 成功转换: {filename}")
            else:
                tqdm.write(f"⚠️ 警告: {filename} 转换后未生成文件")
                
        except Exception as e:
            tqdm.write(f"❌ 转换 {filename} 失败: {str(e)}")
            continue
    
    print("转换完成!")

def get_folder_path(prompt):
    """交互式获取并验证文件夹路径"""
    while True:
        path = input(prompt).strip().strip('"').strip("'")
        if os.path.isdir(path):
            return path
        if "输出" in prompt:
            create = input(f"文件夹不存在，是否创建? (y/n): ").strip().lower()
            if create == 'y':
                os.makedirs(path, exist_ok=True)
                return path
        print(f"路径无效，请重新输入")

if __name__ == "__main__":
    print("===== DTA → Parquet 转换器 =====")
    input_folder =""
    output_folder =""
    chunksize = 20000
    convert_dta_to_parquet(input_folder, output_folder, chunksize)