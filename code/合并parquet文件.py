# 说明：
# 这是一个将一个文件夹中的.parquet文件合并成一个文件的程序
# 在if __name__ == "__main__":处进行修改
# 可以选择同时生成一个.csv文件
# 参数:
#   folder_path (str): 待合并Parquet文件所在的文件夹路径
#   output_file (str): 合并后Parquet文件的保存路径（默认：当前目录下merged.parquet）
#   include_subfolders (bool): 是否包含子文件夹中的Parquet文件（默认：False）
#   csv_output_file (str): 合并后CSV文件的保存路径（默认：None，即不转换为CSV）
import pandas as pd
from pathlib import Path
import os

def merge_parquet_files(folder_path, output_file='merged.parquet', include_subfolders=False, csv_output_file=None):

    # 文件夹是否存在
    if not os.path.isdir(folder_path):
        raise ValueError(f"文件夹不存在: {folder_path}")
    
    # 查找所有Parquet文件
    if include_subfolders:
        parquet_files = list(Path(folder_path).rglob('*.parquet'))  # 包含子文件夹
    else:
        parquet_files = list(Path(folder_path).glob('*.parquet'))   # 仅当前文件夹
    
    if not parquet_files:
        print(f"未找到Parquet文件（格式为.parquet）")
        return
    
    print(f"找到 {len(parquet_files)} 个Parquet文件，开始合并...")
    
    # 读取所有Parquet文件到DataFrame列表
    dfs = []
    for file in parquet_files:
        try:
            print(f"正在读取: {file.name}")
            df = pd.read_parquet(file, engine='pyarrow')
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ 读取文件 {file.name} 失败: {str(e)}，已跳过")
    
    if not dfs:
        print("❌ 所有文件读取失败，合并未完成")
        return
    
    # 合并所有DataFrame（忽略原索引，重新生成索引）
    merged_df = pd.concat(dfs, ignore_index=True)
    print(f"✅ 合并完成！共 {len(merged_df)} 行数据")
    
    # 保存合并后的Parquet文件
    merged_df.to_parquet(output_file, engine='pyarrow')
    print(f"📁 Parquet文件已保存至: {os.path.abspath(output_file)}")
    
    # 若指定了CSV输出路径，则转换为CSV
    if csv_output_file:
        try:
            merged_df.to_csv(csv_output_file, index=False, encoding='utf-8')
            print(f"📄 CSV文件已保存至: {os.path.abspath(csv_output_file)}")
        except Exception as e:
            raise Exception(f"CSV文件保存失败: {str(e)}")


if __name__ == "__main__":
    print("===== Parquet文件合并与CSV转换工具 =====")
    folder_path = input("请输入需要合并的parquet文件夹位置")
    output_file = input("请为生成的parquet文件命名")
    include_subfolders = input("是否包含子文件夹中的Parquet文件？(y/n，默认n): ").strip().lower() == 'y'
    
    # 询问是否转换为CSV
    convert_to_csv = input("是否将合并后的文件转换为CSV文件？(y/n，默认n): ").strip().lower() == 'y'
    csv_output_file = None
    if convert_to_csv:
        csv_output_file = input("请输入CSV文件的保存路径（默认：合并文件同名.csv）: ").strip()
        if not csv_output_file:
            # 自动生成CSV文件名（替换Parquet的扩展名）
            csv_output_file = os.path.splitext(output_file)[0] + '.csv'
    
    try:
        merge_parquet_files(
            folder_path=folder_path,
            output_file=output_file,
            include_subfolders=include_subfolders,
            csv_output_file=csv_output_file
        )
    except Exception as e:
        print(f"❌ 操作失败: {str(e)}")