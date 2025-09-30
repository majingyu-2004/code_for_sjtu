# 将一个parquet文件转换为csv文件
# 也可使用“合并parquet文件.py”程序
import pandas as pd

input_p = input("输入文件位置：").replace("\"","")
output_p = input("输出文件位置：").replace("\"","")
input_path = rf"{input_p}"
output_path = rf"{output_p}"
# 读取Parquet文件
df = pd.read_parquet(input_path)  # 替换为你的文件路径
# 保存为CSV（自动处理中文）
df.to_csv(output_path, index=False, encoding='utf_8_sig')
print("转换完成！")