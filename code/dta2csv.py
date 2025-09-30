# 将DTA文件中的特定一列转换为CSV格式
# 参数:
#   dta_path (str): DTA文件路径（如'数据.dta'）
#   column_name (str): 需要提取的列名（如'年龄'、'销售额'）
#   output_csv_path (str): 输出CSV文件路径（如'年龄列.csv'）
# 使用deepseek-v3编写

import pandas as pd

def dta_column_to_csv(dta_path, column_name, output_csv_path):

    # 1. 读取DTA文件
    try:
        data = pd.read_stata(dta_path)
    except Exception as e:
        raise ValueError(f"读取DTA文件失败：{e}")
    
    # 2. 检查列名是否存在
    if column_name not in data.columns:
        raise ValueError(f"列名 '{column_name}' 不在DTA文件中！可用列名：{data.columns.tolist()}")
    
    # 3. 提取特定列（保持为DataFrame，保留表头）
    selected_column = data[[column_name]]  # 双括号确保返回DataFrame（而非Series）
    
    # 4. 保存为CSV（不保存索引，使用UTF-8编码）
    try:
        selected_column.to_csv(output_csv_path, index=False, encoding='utf-8')
        print(f"成功将列 '{column_name}' 保存到：{output_csv_path}")
    except Exception as e:
        raise ValueError(f"保存CSV失败：{e}")

# ---------------------- 示例用法 ----------------------
if __name__ == "__main__":
    # 替换为你的文件路径和列名
    dta_f = input("").replace("\"","")
    output_c = input("").replace("\"","")
    dta_file = rf"{dta_f}"      # 输入DTA文件
    output_csv = rf"{output_c}" # 输出CSV文件
    target_column = "name"    # 需要提取的列名（如'gender'、'income'）

    dta_column_to_csv(dta_file, target_column, output_csv)