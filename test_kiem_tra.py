import polars as pl                # Thư viện xử lý bảng dữ liệu
import datetime                  # Thư viện ngày tháng
import duckdb                    # Thư viện truy vấn SQL
from storage_engine import save_to_historical # Hàm lưu file Parquet
from db_engine import DuckDBEngine # Bộ não truy vấn dữ liệu lịch sử
import os                        # Thư viện hệ thống tệp
import shutil                    # Thư viện thao tác thư mục (để dọn dẹp data cũ)

# BƯỚC CHUẨN BỊ: Xóa sạch kho dữ liệu cũ để bài Test mang tính khách quan nhất
historical_dir = os.path.join("data", "historical")
if os.path.exists(historical_dir):
    shutil.rmtree(historical_dir) # Lệnh xóa toàn bộ thư mục data/historical

# --- GIẢ LẬP KỊCH BẢN NGÀY 1: CHỈ CÓ 3 CHIẾN DỊCH (C1, C2, C3) ---
df_ngay_1 = pl.DataFrame({
    "Entity": ["Campaign", "Campaign", "Campaign"],
    "Campaign Id": ["C1", "C2", "C3"],
    "Ad Group Id": [None, None, None],
    "Keyword Id": [None, None, None],
    "Product Targeting Id": [None, None, None],
    "Campaign Name": ["Camp X", "Camp Y", "Camp Z"],
    "Clicks": [10, 20, 30] # Số lượt Click ban đầu
})

print("--- 📂 GIẢ LẬP FILE BÁO CÁO NGÀY 1 (Nạp 3 Campaign) ---")
print(df_ngay_1)
# Lưu vào kho Parquet dưới nhãn ngày 01/04/2026
save_to_historical(df_ngay_1, "20260401")

# --- GIẢ LẬP KỊCH BẢN NGÀY 2: DỮ LIỆU CẬP NHẬT VÀ CÓ THÊM CHIẾN DỊCH MỚI (C4) ---
# Trong thực tế, hôm sau ta nạp file mới thì C1, C2, C3 vẫn còn đó nhưng số Click đã tăng lên
df_ngay_2 = pl.DataFrame({
    "Entity": ["Campaign", "Campaign", "Campaign", "Campaign"],
    "Campaign Id": ["C1", "C2", "C3", "C4"],
    "Ad Group Id": [None, None, None, None],
    "Keyword Id": [None, None, None, None],
    "Product Targeting Id": [None, None, None, None],
    "Campaign Name": ["Camp X", "Camp Y", "Camp Z", "Camp MỚI TINH"],
    "Clicks": [15, 25, 30, 5] # Clicks của C1 và C2 đã tăng lên, C4 là lính mới
})
print("\n--- 📂 GIẢ LẬP FILE BÁO CÁO NGÀY 2 (Cập nhật C1-C3 và thêm mới C4) ---")
print(df_ngay_2)
# Lưu vào kho Parquet dưới nhãn ngày 02/04/2026
save_to_historical(df_ngay_2, "20260402")

# --- KIỂM CHỨNG: TRUY VẤN DUCKDB XEM CÓ BỊ TRÙNG LẶP DỮ LIỆU KHÔNG ---
print("\n🔥 KẾT QUẢ DUCKDB TRÍCH XUẤT (Phải lấy được trạng thái Mới Nhất của mỗi ID) 🔥")
db = DuckDBEngine() # Khởi tạo kết nối DuckDB
db.create_unified_view() # Gộp 2 file Parquet ngày 1 và ngày 2 lại thành 1 cái nhìn chung
df_ket_qua = db.get_latest_metrics() # Dùng hàm lấy 'Latest Metrics' (Dựa trên ROW_NUMBER() OVER PARTITION BY)

# In kết quả kiểm tra: Mong đợi là chỉ thấy 4 dòng (C1, C2, C3, C4) với số Click của ngày 2
print(df_ket_qua.select(["Report Date", "Entity", "Campaign Id", "Campaign Name", "Clicks"]).sort("Campaign Id"))
db.close() # Đóng kết nối DB
