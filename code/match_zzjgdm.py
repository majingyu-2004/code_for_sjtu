# 这是用于课题中匹配组织机构代码的程序
# 运行地址在C盘-用户-mjy12中，索引文件也在那里
# def match_with_bfile 函数中的SIMILARITY_THRESHOLD可以设置企业名称匹配的相似度下限，相似度低于下限则匹配失败，建议设置为0.85
# def main()中可以设置输入文件地址
# 使用Doubao-Seed-1.6-flash与deepseek-v3编写

import pandas as pd
import numpy as np
import pyarrow as pa
from pyarrow import dataset as ds
from collections import defaultdict
from difflib import SequenceMatcher
import os
import sys
import glob
import gc  # 垃圾回收模块
from multiprocessing import Pool, cpu_count
import warnings
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

# ================== 核心优化函数 ==================
def create_index_db(fileB_paths, index_file='.code_index.npy', force_rebuild=False):
    """预处理单个B文件（Parquet格式）为内存索引"""
    if os.path.exists(index_file) and not force_rebuild:
        logger.info(f"加载已存在的索引文件: {index_file}")
        return np.load(index_file, allow_pickle=True).item()
    
    logger.info(f"开始构建机构代码索引（共{len(fileB_paths)}个文件）...")
    index = defaultdict(list)
    
    for file_path in fileB_paths:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"fileB文件不存在: {file_path}")
        logger.info(f"处理文件: {os.path.basename(file_path)}")
        
        try:
            parquet_ds = ds.dataset(file_path, format="parquet")
            
            for batch in parquet_ds.to_batches():
                chunk = batch.to_pandas()
                
                for _, row in chunk.iterrows():
                    try:
                        code = str(row['组织机构代码'])
                        index[code].append((
                        row['企业名称'], 
                        row['newgcid']
                    ))
                    except KeyError as e:
                        logger.warning(f"文件 {os.path.basename(file_path)} 缺少必要列: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"读取文件 {file_path} 出错: {str(e)}", exc_info=True)
            continue
    
    np.save(index_file, np.array(dict(index)))
    logger.info(f"索引构建完成：共包含{len(index)}个组织机构代码，索引文件保存至{index_file}")
    return index

def match_with_bfile(fileA_df, index, batch_size=100000):
    """匹配单个B文件与当前A文件，返回匹配和未匹配结果"""
    matched = []
    unmatched = []
    SIMILARITY_THRESHOLD = 0.8  # 可根据实际数据调整相似度阈值
    
    # 分批次处理A文件以节省内存
    for i in range(0, len(fileA_df), batch_size):
        batch = fileA_df.iloc[i:i+batch_size]
        logger.info(f"正在处理A文件第{i}-{i+batch_size-1}行，共{len(batch)}条记录")
        
        for _, row in batch.iterrows():
            try:
                code = str(row['cardnum'])
                nameA = str(row.get('name', '')).upper()
                
                matches = index.get(code, [])
                if not matches:
                    unmatched.append(row.to_dict())
                    continue
                    
                best_ratio = 0.0
                best_gcid= None  # 改为存储统一社会信用代码

                
                for nameB, gcid in matches:
                    ratio = SequenceMatcher(None, nameA, str(nameB).upper()).ratio()
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_gcid= gcid # 存储统一社会信用代码
                        
                if best_ratio >= SIMILARITY_THRESHOLD:
                    row_data = row.to_dict()
                    row_data['newgcid'] = best_gcid  # 添加统一社会信用代码

                    matched.append(row_data)
                else:
                    unmatched.append(row.to_dict())
                    
            except Exception as e:
                logger.warning(f"处理行数据出错: {str(e)}", exc_info=True)
                continue
    
    return matched, unmatched

def read_parquet(file_path):
    """读取单个Parquet文件"""
    try:
        return pd.read_parquet(file_path, engine='pyarrow')
    except Exception as e:
        logger.error(f"读取Parquet文件 {file_path} 出错: {str(e)}", exc_info=True)
        raise

# ================== 主程序 ==================
def main():
    fileA_path = input("请输入文件A（.parquet）的地址").replace("\"","")
    fileB_path = input("请输入文件夹B的地址").replace("\"","")
    batch_size_input = input("请输入每批量处理的数量，建议为100000")
    output_path =input("请输入导出文件夹的地址").replace("\"","")

    # 配置参数
    INPUT = {
        'fileA': rf"{fileA_path}",
        'fileB_dir': rf"{fileB_path}",
        'index_file_template': ".code_index_B{}.npy",  # 为每个B文件创建独立索引
        'force_rebuild_index': False,
        'batch_size': int(batch_size_input)  # A文件分块处理大小
    }
    OUTPUT_DIR = rf"{output_path}"
    
    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        # 获取所有fileB文件路径，倒序读取
        fileB_dir = INPUT['fileB_dir']
        fileB_paths = glob.glob(
            os.path.join(fileB_dir, "**", "*.parquet"),
            recursive=True
        )
        fileB_paths.sort(reverse=True)  # 倒序读取文件
        logger.info(f"共发现{len(fileB_paths)}个fileB文件，将按倒序处理")
        
        if not fileB_paths:
            raise FileNotFoundError(f"未找到任何fileB文件！检查目录：{fileB_dir}")
        
        # 读取fileA（一次性加载到内存，如果文件过大可考虑分批次处理）
        logger.info(f"开始读取fileA: {INPUT['fileA']}")
        fileA_df = read_parquet(INPUT['fileA'])
        logger.info(f"fileA初始数据量: {len(fileA_df)}条")
        
        # 逐个处理每个B文件
        for b_idx, file_path in enumerate(fileB_paths, 1):
            b_filename = os.path.basename(file_path)  # 获取完整文件名（含扩展名）
            b_basename = os.path.splitext(b_filename)[0]  # 去除扩展名，保留基础名称（如"企业数据2023"）
            logger.info(f"\n===== 开始处理第{b_idx}个B文件: {b_filename}（基础名称：{b_basename}） =====")
            
            # 构建当前B文件的索引（文件名含基础名称，避免冲突）
            index_file = INPUT['index_file_template'].format(b_basename)
            index = create_index_db(
                fileB_paths=[file_path],
                index_file=index_file,
                force_rebuild=INPUT['force_rebuild_index']
            )
            
            # 匹配当前B文件与A文件
            matched, unmatched = match_with_bfile(fileA_df, index, INPUT['batch_size'])
            
            # 保存匹配结果（文件名含B文件基础名称）
            matched_df = pd.DataFrame(matched)
            unmatched_df = pd.DataFrame(unmatched)
            matched_df.to_parquet(
                os.path.join(OUTPUT_DIR, f'matched_{b_basename}.parquet'),
                engine='pyarrow'
            )
            unmatched_df.to_parquet(
                os.path.join(OUTPUT_DIR, f'unmatched_{b_basename}.parquet'),
                engine='pyarrow'
            )
            logger.info(f"保存B文件匹配结果: 匹配成功{len(matched)}条, 未匹配{len(unmatched)}条")
            
            # 从A文件中剔除匹配成功的数据（保留未匹配数据继续匹配下一个B文件）
            fileA_df = unmatched_df
            logger.info(f"剩余未匹配A文件数据量: {len(fileA_df)}条")
            
            # 清理内存（删除临时变量，强制垃圾回收）
            del index, matched, unmatched, matched_df, unmatched_df
            gc.collect()
    
        # 保存最终未匹配数据（如果还有剩余）
        if not fileA_df.empty:
            fileA_df.to_parquet(
                os.path.join(OUTPUT_DIR, 'remaining_unmatched.parquet'),
                engine='pyarrow'
            )
            logger.info(f"所有B文件处理完成！剩余未匹配数据: {len(fileA_df)}条，已保存至remaining_unmatched.parquet")
    
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    if sys.platform == 'win32':
        from multiprocessing import freeze_support
        freeze_support()  # Windows多进程支持
    main()