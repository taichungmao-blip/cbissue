# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import urllib3
import tempfile
import os
import yfinance as yf  # 新增 yfinance 模組來取得股價
from urllib.parse import urljoin # 記得在最上面 import 這個

# 關閉 SSL 憑證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 設定區塊 ---
# 從 GitHub Actions 的環境變數 (Secrets) 中讀取 Webhook 網址
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
NOTIFIED_FILE = "notified_cbs.txt"

def load_notified_records():
    """讀取已經通知過的紀錄"""
    if not os.path.exists(NOTIFIED_FILE):
        return set()
    with open(NOTIFIED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f)

def save_notified_record(record_id):
    """將新通知的案件寫入紀錄檔"""
    with open(NOTIFIED_FILE, "a", encoding="utf-8") as f:
        f.write(f"{record_id}\n")

def send_discord_notify(message):
    """發送 Discord 通知"""
    if not DISCORD_WEBHOOK_URL:
        print("尚未設定 Discord Webhook 網址 (環境變數遺失)，略過通知。")
        return
        
    data = {"content": message}
    response = requests.post(DISCORD_WEBHOOK_URL, json=data)
    if response.status_code == 204:
        print("Discord 通知發送成功！")
    else:
        print(f"Discord 通知發送失敗，狀態碼: {response.status_code}")

def get_115_fsc_excel_data():
    """爬取金管會 115 年度申報案件的 Excel 檔案"""
    url = "https://www.sfb.gov.tw/ch/home.jsp?id=1016&parentpath=0,6,52"
    
    # 建議加入基本的 User-Agent，降低被阻擋的機率
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    resp = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(resp.text, "html.parser")
    
    tables = soup.find_all("table", {"class": "table01 table02"})
    trs = tables[0].find_all("tr")
    
    # 抓取第 3 列，第 5 欄裡面的 EXCEL 下載連結
    tds = trs[2].find_all("td") 
    raw_file_url = tds[4].find("a").get("href")
    
    # 【修改點 1】確保 URL 是完整的絕對路徑
    file_url = urljoin(url, raw_file_url)
    print(f"準備下載的檔案網址: {file_url}") # 讓 GitHub Actions 留存紀錄
    
    file_resp = requests.get(file_url, headers=headers, verify=False)
    file_resp.raise_for_status()
    
    # 【除錯點】印出下載內容的前 100 個字元，確認是不是被擋或是抓到 HTML
    print(f"下載檔案前 100 Bytes: {file_resp.content[:100]}")
    
    ext = '.xlsx' if '.xlsx' in file_url.lower() else '.xls'
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(file_resp.content)
        tmp_path = tmp.name
        
    try:
        # 【修改點 2】動態決定 engine。如果是 .xls，不能用 openpyxl
        # 註: 若要讀取舊版 .xls，你的環境需要安裝 xlrd 套件 (pip install xlrd)
        if ext == '.xlsx':
            df = pd.read_excel(tmp_path, header=2, engine='openpyxl')
        else:
            df = pd.read_excel(tmp_path, header=2, engine='xlrd') # 或者不寫 engine 讓 pandas 自動判斷
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            
    return df
    
def get_col_name(columns, keyword):
    """輔助函式：用關鍵字找實際的欄位名稱 (模糊比對)"""
    for col in columns:
        if keyword in str(col):
            return col
    return None

def get_stock_price(stock_code, company_type):
    """利用 yfinance 取得最新股價"""
    company_type = str(company_type)
    # 根據上市櫃決定後綴，興櫃股票支援度較差直接略過
    if '上市' in company_type:
        symbol = f"{stock_code}.TW"
    elif '上櫃' in company_type:
        symbol = f"{stock_code}.TWO"
    else:
        return "暫不支援 (非上市櫃)"
        
    try:
        ticker = yf.Ticker(symbol)
        # 取得最近一天的交易資料
        hist = ticker.history(period="1d")
        if not hist.empty:
            price = hist['Close'].iloc[-1]
            return f"{price:.2f}"
        else:
            return "查無報價"
    except Exception:
        return "取得失敗"

if __name__ == "__main__":
    df_data = get_115_fsc_excel_data()
    notified_records = load_notified_records()
    
    # 清理欄位名稱 (去除換行與空白)
    df_data.columns = df_data.columns.astype(str).str.replace('\n', '').str.replace(' ', '')
    
    # 動態抓取實際欄位名稱
    col_target = get_col_name(df_data.columns, '案件類別')
    col_company = get_col_name(df_data.columns, '公司名稱')
    col_code = get_col_name(df_data.columns, '代號')
    col_type = get_col_name(df_data.columns, '型態')
    col_amount = get_col_name(df_data.columns, '金　　　　額')
    col_currency = get_col_name(df_data.columns, '幣別')
    col_receipt = get_col_name(df_data.columns, '收文日期')
    col_effective = get_col_name(df_data.columns, '生效日期')
    
    if col_target:
        # 篩選包含「轉換公司債」的資料 (忽略空值)
        cb_data = df_data[df_data[col_target].astype(str).str.contains('轉換公司債', na=False)]
        
        if cb_data.empty:
            print("目前沒有轉換公司債的案件。")
        
        for index, row in cb_data.iterrows():
            company_name = row[col_company] if col_company else '未知公司'
            case_type = row[col_target] if col_target else '未知案件'
            company_type = row[col_type] if col_type else '未知'
            currency = row[col_currency] if col_currency else '未知'
            receipt_date = row[col_receipt] if col_receipt else '未知'
            
            # --- 處理代號：確保移除 pandas 讀取數字時可能產生的 .0 ---
            stock_code_raw = str(row[col_code]) if col_code else '未知'
            stock_code = stock_code_raw.split('.')[0] if stock_code_raw != '未知' else '未知'
            
            # --- 處理生效日期：移除結尾的 .0 ---
            effective_date = str(row[col_effective]) if col_effective else '未知'
            if effective_date.endswith('.0'):
                effective_date = effective_date[:-2]
            
            # --- 處理金額：轉換為「億」 ---
            amount_val = row[col_amount] if col_amount else '未知'
            if str(amount_val) != '未知':
                try:
                    # 轉成字串並去除千分位逗號，再轉成浮點數計算
                    clean_amount = float(str(amount_val).replace(',', '').strip())
                    
                    # 自動判斷 Excel 欄位名稱是否帶有「仟元」或「萬元」等單位
                    if '仟' in str(col_amount) or '千' in str(col_amount):
                        amount_in_yi = clean_amount / 100000
                    elif '萬' in str(col_amount):
                        amount_in_yi = clean_amount / 10000
                    else:
                        amount_in_yi = clean_amount / 100000000
                    
                    # 格式化輸出，最多顯示小數點後兩位，並去掉結尾多餘的 0 和小數點
                    amount = f"{amount_in_yi:.2f}".rstrip('0').rstrip('.') + " 億"
                except (ValueError, TypeError):
                    # 如果資料是純文字或異常導致無法計算，就維持原樣
                    amount = str(amount_val)
            else:
                amount = '未知'
            
            # --- 取得最新股價 ---
            stock_price = get_stock_price(stock_code, company_type)
            
            # --- 建立 Yahoo 技術分析連結 ---
            yahoo_tech_url = f"https://tw.stock.yahoo.com/quote/{stock_code}/technical-analysis"
            
            # 建立唯一識別碼 (加入收文日期，避免同公司不同次發行被略過)
            record_id = f"{company_name}_{case_type}_{receipt_date}"
            
            if record_id not in notified_records:
                # 組裝訊息：加入股價、連結，並移除資料來源備註
                msg = (
                    f"🔔 **新轉換公司債案件通知** 🔔\n"
                    f"**證券代號**：{stock_code}\n"
                    f"**公司名稱**：{company_name}\n"
                    f"**公司型態**：{company_type}\n"
                    f"**案件類別**：{case_type}\n"
                    f"**金額**：{amount}\n"
                    f"**幣別**：{currency}\n"
                    f"**收文日期**：{receipt_date}\n"
                    f"**生效日期**：{effective_date}\n"
                    f"**最新股價**：{stock_price}\n"
                    f"**技術分析**：{yahoo_tech_url}"
                )
                
                send_discord_notify(msg)
                
                # 紀錄起來，下次就不會再通知
                save_notified_record(record_id)
                notified_records.add(record_id)
            else:
                print(f"[{record_id}] 已通知過，略過。")
    else:
        print("找不到包含 '案件類別' 的欄位。")
        print("目前的欄位有：", df_data.columns.tolist())
