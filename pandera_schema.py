import polars as pl       # Thư viện xử lý dữ liệu mạnh mẽ dựa trên ngôn ngữ Rust
import sys               # Thư viện hệ thống của Python

# Đảm bảo in các ký tự Emoji hoặc Tiếng Việt ra Terminal không bị lỗi (quy chuẩn UTF-8)
sys.stdout.reconfigure(encoding='utf-8')

# Danh sách 52+ cột tiêu chuẩn của Amazon Sponsored Products Bulksheets.
# Việc định nghĩa này giúp hệ thống biết được một file "Đủ bộ" là như thế nào.
# Thêm các cột Report Date để phục vụ lưu trữ lịch sử Time-Series.
AMAZON_BULKSHEET_COLUMNS = [
    "Product", "Entity", "Operation", "Campaign ID", "Ad Group ID", "Portfolio ID",
    "Ad ID", "Keyword ID", "Product Targeting ID", "Campaign Name", "Ad Group Name",
    "Start Date", "End Date", "Targeting Type", "State", "Daily Budget", "SKU",
    "Ad Group Default Bid", "Bid", "Keyword Text", "Match Type", "Bidding Strategy",
    "Placement", "Percentage", "Product Targeting Expression", "Impressions", "Clicks",
    "Click-Through Rate", "Spend", "Sales", "Orders", "Units", "Conversion Rate", "ACOS",
    "CPC", "ROAS", "Date", "Report Date" # Cột mở rộng để quản lý dữ liệu lịch sử
]

# Hàm reconcile_schema: Đóng vai trò 'Bộ lọc Cửa khẩu' cho dữ liệu
def reconcile_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Nhiệm vụ: 
    1. Sửa lỗi sai chính tả Hoa/Thường của các cột (Campaign ID -> Campaign Id).
    2. Bù đắp các cột còn thiếu bằng giá trị rỗng (Null).
    3. Bảo vệ các cột ID không bị biến dạng thành số khoa học (E+13).
    """
    # Lấy danh sách các cột thực tế đang có trong file người dùng nạp vào
    current_columns = df.columns
    
    # BƯỚC 0: Sửa lỗi Case-insensitive (Hoa/Thường)
    # Tạo một bản đồ (Map) so sánh tên cột viết thường và tên cột chuẩn
    standard_cols_lower = {col.lower(): col for col in AMAZON_BULKSHEET_COLUMNS}
    rename_mapping = {}
    for col in current_columns:
        col_lower = col.lower()
        # Nếu cột hiện tại (ví dụ: 'campaign id') khớp với chuẩn ('Campaign Id') sau khi viết thường
        if col_lower in standard_cols_lower and col != standard_cols_lower[col_lower]:
            rename_mapping[col] = standard_cols_lower[col_lower] # Lưu lại lệnh đổi tên
            
    # Áp dụng đổi tên hàng loạt nếu phát hiện lệch chuẩn Hoa/Thường
    if rename_mapping:
        df = df.rename(rename_mapping)
        current_columns = df.columns # Cập nhật lại danh sách cột sau khi đổi tên
    
    # BƯỚC 1: Tiêm bù các cột còn thiếu (Column Imputation)
    # Tìm xem cột nào trong 'Chuẩn' mà file nạp vào chưa có
    missing_columns = [col for col in AMAZON_BULKSHEET_COLUMNS if col not in current_columns]
    if missing_columns:
        print(f"🔧 [SCHEMA] Phát hiện thiếu {len(missing_columns)} cột, đang tiêm bù Null để đồng bộ hóa...")
        # Lặp qua từng cột thiếu và gắn thêm cột Rỗng (Null/None) vào bảng
        for col in missing_columns:
            df = df.with_columns(pl.lit(None).cast(pl.String).alias(col))
            
    # BƯỚC 2: Đặc trị 'Tử huyệt ID' (ID Integrity)
    # Tìm tất cả các cột kết thúc bằng chữ 'Id' (bao gồm cả 'Campaign Id', 'Keyword Id'...)
    id_cols = [c for c in df.columns if c.lower().endswith(" id") or c.lower() == "id"]
    cast_exprs = [] # Danh sách các công thức chuyển đổi kiểu dữ liệu
    
    for col in id_cols:
        # Nếu cột ID đang ở dạng số thực (Float) - Đây là lỗi Excel hay gặp (số 123456789 thành 1.23e+10)
        if df[col].dtype in [pl.Float64, pl.Float32]:
            # Công thức: Lấp giá trị trống bằng -1 -> Chuyển sang Số nguyên (Int64) -> Chuyển sang Chuỗi (String) -> Trả lại -1 thành Null
            # Cách này giúp ID giữ nguyên được con số gốc mà ko bị phẩy thập phân
            expr = pl.col(col).fill_null(-1).cast(pl.Int64).cast(pl.String).replace("-1", None)
            cast_exprs.append(expr)
        else:
            # Nếu ID đã ở dạng khác, cứ ép về String cho an toàn tuyệt đối
            expr = pl.col(col).cast(pl.String)
            cast_exprs.append(expr)
            
    # Áp dụng các biểu thức ép kiểu ID đã chuẩn bị
    if cast_exprs:
        df = df.with_columns(cast_exprs)
        
    # Trả về DataFrame đã 'sạch sẽ' và đạt chuẩn Amazon Bulksheet quốc tế
    return df

# Khối lệnh chạy thử nghiệm khi bấm 'Run' trực tiếp file này
if __name__ == "__main__":
    # Giả lập một bảng dữ liệu bị lỗi (thiếu cột, ID bị biến dạng số thực)
    test_df = pl.DataFrame({
        "Product": ["Sponsored Products"],
        "Entity": ["Campaign"],
        "Campaign ID": [1.458999e+10], # ID bị lỗi định dạng khoa học
        "Clicks": [112]
    })
    
    print("--- 📥 DỮ LIỆU ĐẦU VÀO (Bị lỗi định dạng và thiếu hụt cột): ---")
    print(test_df)
    
    # Chạy qua bộ lọc Schema
    safe_df = reconcile_schema(test_df)
    
    print("\n--- 📤 DỮ LIỆU ĐẦU RA (Đã được chữa trị và bù đắp cột): ---")
    # Kiểm tra xem các cột thiếu như 'Spend' hay 'SKU' đã được bù Null chưa
    print(safe_df.select(["Product", "Entity", "Campaign ID", "Clicks", "Spend", "SKU"]).head())
    print("\n✅ Kiểm tra ID sau khi ép kiểu:", safe_df["Campaign ID"].to_list())
