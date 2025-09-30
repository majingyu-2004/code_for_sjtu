import csv
import random
import keyboard
import os
import sys

def read_csv_file(file_path):
    """读取CSV文件并返回数据列表"""
    data = []
    try:
        # 尝试使用不同编码读取文件，解决中文乱码问题
        encodings = ['utf-8', 'gbk', 'latin-1']
        for encoding in encodings:
            try:
                with open(file_path, 'r', newline='', encoding=encoding) as file:
                    reader = csv.reader(file)
                    header = next(reader)  # 获取表头
                    data.append(header)    # 将表头作为第一行存入数据
                    
                    for row in reader:
                        data.append(row)
                print(f"成功读取CSV文件，共 {len(data)-1} 行数据（不含表头），使用编码: {encoding}")
                return data
            except UnicodeDecodeError:
                continue
        
        print(f"错误：无法解码文件 '{file_path}'，尝试了多种编码")
        return None
        
    except FileNotFoundError:
        print(f"错误：找不到文件 '{file_path}'")
        return None
    except Exception as e:
        print(f"读取文件时出错：{str(e)}")
        return None

def print_random_rows(data, num_rows=3, columns=None):
    """
    随机打印几行的指定列以及行号
    参数:
    data: 包含CSV数据的列表
    num_rows: 要打印的随机行数，默认为3
    columns: 要打印的列索引列表，默认为None（打印所有列）
    """
    if not data or len(data) <= 1:  # 检查数据是否有效（至少需要表头和一行数据）
        print("没有可打印的数据")
        return
    
    # 清除屏幕
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print("="*70)
    print(f"随机打印 {num_rows} 行数据（行号从1开始计数）：")
    print("="*70)
    
    # 如果没有指定列，默认打印所有列
    if columns is None:
        columns = range(len(data[0]))
    
    # 打印表头
    header = [data[0][col] for col in columns]
    print(f"表头: {', '.join(header)}")
    print("-"*70)
    
    # 随机选择行（排除表头）
    if len(data) - 1 <= num_rows:
        # 如果数据行数小于等于要打印的行数，则打印所有行
        selected_rows = range(1, len(data))
        print(f"注意：数据总行数少于请求行数，已打印所有 {len(selected_rows)} 行数据")
        print("-"*70)
    else:
        # 否则随机选择指定数量的行
        selected_rows = random.sample(range(1, len(data)), num_rows)
    
    # 打印选中的行
    for row_num in selected_rows:
        row_data = [data[row_num][col] for col in columns]
        print(f"行 {row_num}: {', '.join(row_data)}")
    
    print("="*70)
    print("按空格键重新生成随机行，按ESC键退出程序")
    print("="*70)

def main():
    """主函数"""
    print("="*70)
    print("          CSV文件随机行查看器          ")
    print("="*70)
    
    # 获取CSV文件路径
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("请输入CSV文件路径: ").replace("\"","")
        file_path = rf"{file_path}"
    
    # 读取CSV文件
    csv_data = read_csv_file(file_path)
    if not csv_data:
        return
    
    # 显示表头供用户参考
    print("\n表头预览（列索引从0开始）：")
    for i, column_name in enumerate(csv_data[0]):
        print(f"  列 {i}: {column_name}")
    
    # 获取要打印的列
    try:
        columns_input = input("\n请输入要打印的列索引（用逗号分隔，如0,2,4，默认为所有列）: ")
        if columns_input.strip():
            columns = [int(col.strip()) for col in columns_input.split(',')]
            # 验证列索引是否有效
            valid_columns = []
            for col in columns:
                if 0 <= col < len(csv_data[0]):
                    valid_columns.append(col)
                else:
                    print(f"警告：列索引 {col} 无效，将被忽略")
            columns = valid_columns if valid_columns else None
        else:
            columns = None
    except ValueError:
        print("列索引输入无效，将打印所有列")
        columns = None
    
    # 获取要打印的行数
    try:
        num_rows = int(input("请输入要打印的随机行数（默认为3）: ") or "3")
        if num_rows < 1:
            print("行数不能小于1，将使用默认值3")
            num_rows = 3
        elif num_rows > len(csv_data) - 1:
            print(f"行数不能大于数据总行数({len(csv_data)-1})，将使用最大行数")
            num_rows = len(csv_data) - 1
    except ValueError:
        print("行数输入无效，将使用默认值3")
        num_rows = 3
    
    # 首次打印随机行
    print_random_rows(csv_data, num_rows, columns)
    
    # 设置键盘监听
    print("\n程序已启动，按空格键重新生成随机行，按ESC键退出...")
    
    # 定义按键处理函数
    def on_key_press(event):
        if event.name == 'space':
            print_random_rows(csv_data, num_rows, columns)
        elif event.name == 'esc':
            print("\n程序退出，再见！")
            keyboard.unhook_all()
            os._exit(0)
    
    # 监听键盘事件
    keyboard.on_press(on_key_press)
    
    # 保持程序运行
    keyboard.wait()

if __name__ == "__main__":
    main()