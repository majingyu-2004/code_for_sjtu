import pandas as pd
import os
import re
import json
from glob import glob
from tqdm import tqdm

# ==================== 配置参数 ====================
input_dir = r"C:\Users\mjy12\Desktop\企业失信\shuru"
output_dir = r"C:\Users\mjy12\Desktop\企业失信\shuchu"
jsonl_dir = r"C:\Users\mjy12\Desktop\deepseek结果"

min_length = 20         # 最小duty长度
min_year = 2014         # 最小年份
CHUNK_SIZE = 100000     # CSV分块读取大小
NUM_PARTS = 10          # 最终清洗数据分块数

# ==================== 初始化 ====================
os.makedirs(output_dir, exist_ok=True)
removed_file = os.path.join(output_dir, "企业删除数据.csv")
if os.path.exists(removed_file):
    os.remove(removed_file)

total_clean = 0
total_removed = 0
final_clean_chunks = []

# ==================== 清洗数据主流程 ====================
file_names = ["qiye_all_part1.csv", "qiye_all_part2.csv", "qiye_all_part3.csv"]

print("开始清洗数据...")
for file_name in file_names:
    file_path = os.path.join(input_dir, file_name)
    try:
        for chunk in tqdm(pd.read_csv(file_path, chunksize=CHUNK_SIZE), 
                          desc=f"处理 {file_name}"):
            # 数据清洗条件
            length_mask = chunk['duty'].notna() & (chunk['duty'].str.len() >= min_length)
            year_mask = (chunk['publish_year'] >= min_year) & (chunk['case_year'] >= min_year)
            valid_mask = length_mask & year_mask

            # 分割有效/无效数据
            df_clean = chunk[valid_mask].copy()
            df_removed = chunk[~valid_mask]

            # 保存被删除数据（追加模式，仅首行写表头）
            df_removed.to_csv(removed_file, mode='a', index=False, 
                             header=not os.path.exists(removed_file))

            # 收集有效数据块
            final_clean_chunks.append(df_clean)
            total_clean += len(df_clean)
            total_removed += len(df_removed)

    except Exception as e:
        print(f"处理文件 {file_name} 出错: {str(e)}")
        continue

# 合并所有有效数据
final_clean_data = pd.concat(final_clean_chunks, ignore_index=True)

# ==================== 分块保存并添加局部ID（关键修改！） ====================
chunk_size = len(final_clean_data) // NUM_PARTS
for part_idx in range(NUM_PARTS):
    start = part_idx * chunk_size
    end = start + chunk_size if part_idx < NUM_PARTS - 1 else len(final_clean_data)
    part = final_clean_data.iloc[start:end].copy()
    
    # 为每个分块添加局部ID（从1开始递增）
    part['local_id'] = range(1, len(part) + 1)  # 分块内局部唯一ID
    
    part_path = os.path.join(output_dir, f"个人清洗数据_分块_{part_idx+1}.csv")
    part.to_csv(part_path, index=False, encoding='utf-8')

print(f"\n清洗完成！有效数据：{total_clean}条，被删除数据：{total_removed}条")

# ==================== 关联JSONL内容（基于分块局部ID） ====================
def extract_part_num(filename):
    """提取JSONL文件名中的分块编号（如'qy-prompt1-3.jsonl'提取'3'）"""
    nums = re.findall(r'(\d+)', filename)
    return nums[-1] if nums else None

# 构建JSONL文件映射（分块编号 -> 文件路径）
jsonl_pattern1 = os.path.join(jsonl_dir, "qy-prompt1-*.jsonl")
jsonl_pattern2 = os.path.join(jsonl_dir, "qy-prompt2-*.jsonl")
jsonl_files1 = sorted(glob(jsonl_pattern1))
jsonl_files2 = sorted(glob(jsonl_pattern2))

jsonl_map1 = {extract_part_num(f): f for f in jsonl_files1 if extract_part_num(f)}
jsonl_map2 = {extract_part_num(f): f for f in jsonl_files2 if extract_part_num(f)}

# 处理每个分块（基于局部ID匹配）
for part_idx in range(1, NUM_PARTS + 1):
    part_num = str(part_idx)
    part_path = os.path.join(output_dir, f"个人清洗数据_分块_{part_num}.csv")
    
    # 读取分块数据（含local_id）
    df = pd.read_csv(part_path, encoding='utf-8')
    if 'local_id' not in df.columns:
        print(f"警告：分块{part_num}缺少'local_id'列，可能是分块阶段错误！")
        continue

    # 获取对应的JSONL文件（分块编号必须一致）
    jsonl_file1 = jsonl_map1.get(part_num)
    jsonl_file2 = jsonl_map2.get(part_num)
    print(f"\n处理分块{part_num}：")
    print(f"  prompt1文件: {jsonl_file1 if jsonl_file1 else '未找到'}")
    print(f"  prompt2文件: {jsonl_file2 if jsonl_file2 else '未找到'}")

    # 解析JSONL并构建local_id->content映射（关键修改！）
    def parse_jsonl(file_path):
        local_id_content = {}
        if not file_path or not os.path.exists(file_path):
            return local_id_content
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                        # 提取custom_id中的局部ID（如'request-5'提取5）
                        custom_id = item.get('custom_id', '')
                        match = re.search(r'\d+', str(custom_id))
                        if not match:
                            continue
                        local_id = int(match.group())  # 分块内局部ID
                        
                        # 鲁棒提取content（覆盖多种JSON结构）
                        content = None
                        if 'response' in item:
                            body = item['response'].get('body', {})
                            choices = body.get('choices', [])
                            if choices:
                                message = choices[0].get('message', {})
                                content = message.get('content')
                        elif 'message' in item:
                            content = item['message'].get('content')
                        elif 'content' in item:
                            content = item.get('content')
                        
                        if content:
                            local_id_content[local_id] = content
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"  解析{file_path}出错: {str(e)}")
        return local_id_content

    # 构建两个prompt的映射（基于分块内local_id）
    id_content1 = parse_jsonl(jsonl_file1)
    id_content2 = parse_jsonl(jsonl_file2)

    # 关联content到分块数据（用local_id匹配）
    df['content1'] = df['local_id'].map(id_content1)
    df['content2'] = df['local_id'].map(id_content2)

    # 统计匹配情况
    total = len(df)
    matched1 = df['content1'].notna().sum()
    matched2 = df['content2'].notna().sum()
    print(f"  分块数据量: {total}")
    print(f"  content1匹配数: {matched1}（未匹配数: {total - matched1}）")
    print(f"  content2匹配数: {matched2}（未匹配数: {total - matched2}）")

    # 检查是否完全匹配（根据需求调整）
    if matched1 != total or matched2 != total:
        print(f"  警告：分块{part_num}存在未匹配的content！")
    else:
        print(f"  分块{part_num} content1和content2全部匹配成功！")

    # 保存结果（覆盖原分块文件）
    df.to_csv(part_path, index=False, encoding='utf-8-sig')

print("\n所有分块处理完成！")