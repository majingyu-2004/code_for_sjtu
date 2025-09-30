import pandas as pd

# 获取用户输入
input_file = input("请输入导入文件（.csv）的地址：").replace("\"","")
output_file = input("请输入导出文件（.csv）的地址：").replace("\"","")
n = input("请输入需要抽取几行：")
columns = input("请输入要保存的列名（用逗号分隔，例如：姓名,年龄）: ").strip().split(',')
columns = [col.strip() for col in columns]  # 去除列名两边的空格


try:
    df = pd.read_csv(input_file)
    
    # 处理行数n（确保不超过数据总行数）
    n = min(n, len(df))
    
    # 检查列名是否存在
    invalid_cols = [col for col in columns if col not in df.columns]
    if invalid_cols:
        print(f"错误：以下列名不存在于数据中：{invalid_cols}")
        print(f"数据中包含的列名：{df.columns.tolist()}")
        exit()
    
    # 随机抽取n行并选择特定列
    result = df.sample(n=n).loc[:, columns]
    
    result.to_csv(output_file, index=False)
    print(f"成功！已将{len(result)}行数据（{columns}列）保存到 {output_file}")

except FileNotFoundError:
    print(f"错误：文件 '{input_file}' 不存在，请检查路径是否正确")
except Exception as e:
    print(f"发生错误：{e}")