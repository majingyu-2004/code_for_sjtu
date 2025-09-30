import csv
import sys
import os

def count_keywords_in_csv(file_path):
    """
    读取CSV文件并统计关键词出现次数
    csv文件为单列
    请在counters处修改关键词
    """
    # 初始化计数器
    counters = {
        "a": 0,
        "b": 0,
        "c": 0
    }
    
    # 尝试使用不同编码读取文件
    encodings = ['utf-8', 'gbk', 'latin-1', 'utf-16']
    csv_data = None
    encoding_used = None
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as file:
                reader = csv.reader(file)
                csv_data = list(reader)
                encoding_used = encoding
                break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"使用编码 {encoding} 读取文件时出错: {str(e)}")
            continue
    
    if csv_data is None:
        print(f"错误：无法解码文件 '{file_path}'，尝试了多种编码")
        return None
    
    print(f"成功读取CSV文件，共 {len(csv_data)} 行数据，使用编码: {encoding_used}")
    
    # 检查CSV是否只有一列
    if len(csv_data) > 0 and len(csv_data[0]) > 1:
        print(f"警告：CSV文件包含 {len(csv_data[0])} 列数据，程序将只处理第一列")
    
    # 确保tqdm正确导入
    try:
        from tqdm import tqdm as progress_bar
    except ImportError:
        print("tqdm库导入失败，将不显示进度条")
        # 创建一个简单的替代函数
        def progress_bar(iterable, **kwargs):
            return iterable
    
    # 处理每一行数据，显示进度条
    for row in progress_bar(csv_data, desc="处理进度", unit="行"):
        if row:  # 跳过空行
            text = row[0]  # 只处理第一列
            # 检查每个关键词
            for keyword in counters.keys():
                if keyword in text:
                    counters[keyword] += 1
    
    return counters

def main():
    """主函数"""
    print("="*70)
    print("          CSV文件关键词统计工具          ")
    print("="*70)
    
    # 获取CSV文件路径
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("请输入CSV文件路径: ")
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"错误：文件 '{file_path}' 不存在")
        return
    
    # 检查文件是否为CSV格式
    if not file_path.lower().endswith('.csv'):
        print(f"警告：文件 '{file_path}' 不是CSV格式，仍将尝试读取")
    
    # 统计关键词
    results = count_keywords_in_csv(file_path)
    
    if results:
        print("\n" + "="*70)
        print("统计结果:")
        print("-"*70)
        for keyword, count in results.items():
            print(f"{keyword}: {count} 次")
        print("="*70)

if __name__ == "__main__":
    main()