import pandas as pd
import pyreadstat

def csv_to_dta_advanced(csv_file_path, dta_file_path, encoding='utf-8'):
    """  
    参数:
    csv_file_path (str): 输入的CSV文件路径
    dta_file_path (str): 输出的DTA文件路径
    encoding (str): 文件编码格式
    """
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_file_path, encoding=encoding)
        
        # 显示数据基本信息
        print(f"数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")
        print(f"数据类型:\n{df.dtypes}")
        
        # 处理可能的问题
        # 1. 处理缺失值（Stata需要特定的缺失值表示）
        # 2. 确保字符串列长度合适（Stata限制为244字符）
        for col in df.columns:
            if df[col].dtype == 'object':
                # 截断过长的字符串
                df[col] = df[col].astype(str).str.slice(0, 244)
            # 将pandas的NaN转换为None，以便pyreadstat正确处理
            df[col] = df[col].where(pd.notnull(df[col]), None)
        
        # 保存为DTA文件 - 修正这里
        # 使用正确的参数
        pyreadstat.write_dta(df, dta_file_path)
        
        print(f"转换成功！")
        print(f"输入文件: {csv_file_path}")
        print(f"输出文件: {dta_file_path}")
        
        # 验证输出文件
        df_dta, meta = pyreadstat.read_dta(dta_file_path)
        print(f"验证: 输出的DTA文件包含 {len(df_dta)} 行和 {len(df_dta.columns)} 列")
        
    except Exception as e:
        print(f"转换过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
# 使用示例
if __name__ == "__main__":
    csv_f = input("输入csv文件位置：").replace("\"","")
    dta_f = input("输出dta文件位置：").replace("\"","")
    csv_file = rf"{csv_f}"
    dta_file = rf"{dta_f}"
    
    csv_to_dta_advanced(csv_file, dta_file)