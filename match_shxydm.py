import pandas as pd
import numpy as np
import pyarrow as pa
from pyarrow import dataset as ds
from collections import defaultdict
from difflib import SequenceMatcher
import os
import sys
import glob
import gc
import logging
import warnings

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore')

# ================== 核心优化函数 ==================
def create_index_db(fileB_paths, index_file='.credit_index.npy', force_rebuild=False):
    """预处理B文件为内存索引（匹配前10位+后4位信用代码，跳过缺失/无效代码）"""
    if os.path.exists(index_file) and not force_rebuild:
        logger.info(f"加载已存在的索引文件: {index_file}")
        return np.load(index_file, allow_pickle=True).item()
    
    logger.info(f"开始构建信用代码索引（共{len(fileB_paths)}个文件）...")
    index = defaultdict(list)  # 键: 前10位+后4位信用代码, 值: [(企业名称, 县代码)]
    
    for file_path in fileB_paths:
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            continue
        logger.info(f"处理文件: {os.path.basename(file_path)}")
        
        try:
            parquet_ds = ds.dataset(file_path, format="parquet")
            
            for batch in parquet_ds.to_batches():
                chunk = batch.to_pandas()
                
                for _, row in chunk.iterrows():
                    try:
                        # 安全获取信用代码，缺失则跳过
                        credit_code = row.get('统一社会信用代码', np.nan)
                        if pd.isna(credit_code):
                            continue
                        credit_code_str = str(credit_code).strip()
                        
                        # 仅处理长度≥18位的有效信用代码
                        if len(credit_code_str) < 18:
                            continue
                        
                        # 提取前10位（0-9）和后4位（14-17），中间4位（10-13）是有效数据
                        prefix = credit_code_str[:10]
                        suffix = credit_code_str[14:]  # 15-18位（索引14-17）
                        key = f"{prefix}{suffix}"


                        # 可修改可修改可修改可修改可修改可修改可修改可修改可修改可修改可修改可修改
                        # 将你需要的东西合并的东西加入索引，企业名称用于检查相似度
                        # 同时修改def match_with_bfile(fileA_df, index, batch_size=100000):
                        index[key].append((
                        row['企业名称'], 
                        row['newgcid']      # 添加组织机构代码
                    ))
                        
                    except Exception as e:
                        logger.debug(f"行处理异常（跳过）: {str(e)}")
                        continue
                        
        except Exception as e:
            logger.error(f"读取文件 {os.path.basename(file_path)} 出错: {str(e)}")
            continue
    
    np.save(index_file, np.array(dict(index)))
    logger.info(f"索引构建完成：共包含{len(index)}个有效信用代码索引，索引文件保存至{index_file}")
    return index

def match_with_bfile(fileA_df, index, batch_size=100000):
    """匹配A文件与B文件索引，返回匹配结果"""
    matched = []
    unmatched = []
    SIMILARITY_THRESHOLD = 0.9  # 相似度阈值（90%）
    
    for i in range(0, len(fileA_df), batch_size):
        batch = fileA_df.iloc[i:i+batch_size]
        logger.info(f"正在处理A文件第{i}-{i+batch_size-1}行，共{len(batch)}条记录")
        
        for _, row in batch.iterrows():
            try:
                cardnum = str(row['cardnum'])
                if len(cardnum) < 18:  # cardnum必须是18位才能提取有效部分
                    unmatched.append(row.to_dict())
                    continue
                
                # 提取前10位（0-9）和后4位（14-17），中间4位（10-13）是星号
                prefix = cardnum[:10]  # 关键修改：前10位
                suffix = cardnum[14:]  # 后4位（14-17）
                key = f"{prefix}{suffix}"
                
                if not key:
                    unmatched.append(row.to_dict())
                    continue
                    
                matches = index.get(key, [])
                if not matches:
                    unmatched.append(row.to_dict())
                    continue
                    
                best_similarity = 0.0
                #以下是需要合并的项目
                best_gcid = None     # 改为存储组织机构代码
                nameA = str(row.get('name', '')).upper()
                
                for match in matches:
                    #需要修改解包值
                    nameB, gcid = match  # 解包三个值
                    nameB_upper = str(nameB).upper()
                    ratio = SequenceMatcher(None, nameA, nameB_upper).ratio()
                    if ratio > best_similarity:
                        best_similarity = ratio
                        best_gcid = gcid        # 存储组织机构代码
                
                if best_similarity >= SIMILARITY_THRESHOLD:
                    row_data = row.to_dict()
                    row_data['newgcid'] = best_gcid        # 添加组织机构代码
                    matched.append(row_data)
                else:
                    unmatched.append(row.to_dict())
                    
            except Exception as e:
                logger.warning(f"处理行数据出错: {str(e)}")
                continue
    
    return matched, unmatched

# ...（其他函数read_parquet、main保持不变）

def read_parquet(file_path):
    """读取单个Parquet文件"""
    try:
        return pd.read_parquet(file_path, engine='pyarrow')
    except Exception as e:
        logger.error(f"读取Parquet文件 {file_path} 出错: {str(e)}", exc_info=True)
        raise

# ================== 主程序 ==================
def main():
    # 配置参数
    INPUT = {
        'fileA': "C:\\Users\\mjy12\\Desktop\\firm_lawcases\\I_match_merged.parquet",
        'fileB_dir': "F:\\parquet",
        'index_file_template': ".credit_index_{}.npy",  # 模板：添加B文件基础名称作为标识
        'force_rebuild_index': False,
        'batch_size': 100000
    }
    OUTPUT_DIR = "C:\\Users\\mjy12\\Desktop\\firm_lawcases\\final2"
    
    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    try:
        fileB_paths = glob.glob(os.path.join(INPUT['fileB_dir'], "**", "*.parquet"), recursive=True)
        fileB_paths.sort(reverse=True)
        logger.info(f"共发现{len(fileB_paths)}个B文件，将按顺序处理")
        
        if not fileB_paths:
            raise FileNotFoundError(f"未找到任何B文件！检查目录：{INPUT['fileB_dir']}")
        
        logger.info(f"开始读取文件A: {INPUT['fileA']}")
        fileA_df = read_parquet(INPUT['fileA'])
        logger.info(f"文件A初始数据量: {len(fileA_df)}条")
        
        # 逐个处理每个B文件，生成唯一索引
        for b_idx, file_path in enumerate(fileB_paths, 1):
            b_filename = os.path.basename(file_path)
            b_basename = os.path.splitext(b_filename)[0]  # 提取B文件基础名称（不含扩展名）
            logger.info(f"\n===== 开始处理第{b_idx}个B文件: {b_filename}（基础名称：{b_basename}） =====")
            
            # 为当前B文件生成唯一索引文件（避免覆盖）
            index_file = INPUT['index_file_template'].format(b_basename)
            index = create_index_db(
                fileB_paths=[file_path],
                index_file=index_file,
                force_rebuild=INPUT['force_rebuild_index']
            )
            
            # 匹配A文件与当前B文件索引
            matched, unmatched = match_with_bfile(fileA_df, index, INPUT['batch_size'])
            
            # 保存匹配结果（文件名含B文件基础名称，便于追溯）
            matched_df = pd.DataFrame(matched)
            matched_df.to_parquet(
                os.path.join(OUTPUT_DIR, f'matched_{b_filename}'),  # 文件名包含B文件原始名称
                engine='pyarrow'
            )
            logger.info(f"B文件匹配结果: 成功匹配{len(matched)}条, 未匹配{len(unmatched)}条")
            
            # 更新A文件为未匹配数据，继续匹配下一个B文件
            fileA_df = pd.DataFrame(unmatched)
            logger.info(f"剩余未匹配数据量: {len(fileA_df)}条")
            
            # 清理内存
            del index, matched, unmatched, matched_df
            gc.collect()
    
        # 保存最终未匹配数据
        if not fileA_df.empty:
            fileA_df.to_parquet(os.path.join(OUTPUT_DIR, 'remaining_unmatched.parquet'), engine='pyarrow')
            logger.info(f"所有B文件处理完成！剩余未匹配数据: {len(fileA_df)}条")
    
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    if sys.platform == 'win32':
        from multiprocessing import freeze_support
        freeze_support()  # Windows多进程支持
    main()


