# 这是一个获取符合火山方舟批量处理格式jsonl文件的程序
# promptA为固定的提示词，promptB为变化的题干
# json_object处可根据火山方舟的文档设定更具体的参数
# 将处理后的文件存入火山引擎的TOS对象存储，即可在批量推理界面调用

import csv
import json
import os
from tqdm import tqdm
 
# 输入的promptA
promptA =""

# 输入与输出文件的名称,输入文件为csv，输出文件为jsonl
csv_file_name = r""
output_jsonl_file = r""
print(output_jsonl_file)

with open(csv_file_name, mode='r', encoding='utf-8') as csv_file, \
     open(output_jsonl_file, mode='w', encoding='utf-8') as jsonl_file:
        
    csv_reader = csv.reader(csv_file)
    # 读取CSV文件的标题行
    headers = next(csv_reader, None)
    for index, row in enumerate(tqdm(csv_reader)):
         
        # 获取promptB
        promptB = row[9] if headers and row else None
            
        # 如果promptB存在，则创建JSON对象
        if promptB:
        # 生成唯一的custom_id
            custom_id = f"request-{index + 1}"
            json_object = {
                "custom_id": custom_id,
                "body": {
                    "messages": [
                        {"role": "system", "content": promptA},
                        {"role": "user", "content": promptB}
                    ],
                    "temperature": 0
                    }
                }
                
            # 将JSON对象转换为字符串并写入JSONL文件
            jsonl_file.write(json.dumps(json_object, ensure_ascii=False) + '\n')