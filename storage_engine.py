import polars as pl       # Thư viện xử lý dữ liệu bảng (DataFrame) bằng Rust siêu tốc
import os                # Thư viện thao tác với hệ thống tệp và thư mục
import datetime          # Thư viện xử lý ngày tháng
import sys               # Thư viện hệ thống

# Đảm bảo Terminal hiển thị đúng các ký tự đặc biệt và tiếng Việt (UTF-8)
sys.stdout.reconfigure(encoding='utf-8')

# Đường dẫn mặc định nơi chứa kho dữ liệu lịch sử (cấu trúc nén Parquet)
HISTORICAL_DB_PATH = os.path.join("data", "historical")

def save_to_historical(df: pl.DataFrame, report_date_str: str = None) -> bool:
    """
    Hàm chủ chốt thực hiện việc 'Vĩnh cửu hóa' dữ liệu:
    - Chuyển đổi dữ liệu từ DataFrame thành file Parquet (nén Snappy).
    - Phân chia thư mục theo kiểu Partition (Năm/Tháng/Ngày) để DuckDB tìm kiếm nhanh hơn.
    """
    print(f"⏳ [STORAGE] Đang nén dữ liệu vào kho lưu trữ Parquet...")
    
    try:
        # Nếu người dùng không truyền ngày báo cáo, hệ thống tự lấy ngày hôm nay
        if not report_date_str:
            report_date_str = datetime.datetime.now().strftime("%Y%m%d")
            
        # Bóc tách chuỗi ngày (YYYYMMDD) thành Năm, Tháng, Ngày để tạo thư mục
        try:
            year = report_date_str[:4]    # 4 ký tự đầu là Năm
            month = report_date_str[4:6]  # 2 ký tự tiếp theo là Tháng
            day = report_date_str[6:8]    # 2 ký tự cuối là Ngày
        except Exception:
            # Nếu định dạng ngày truyền vào bị lỗi, dùng ngày hiện tại làm phương án dự phòng
            now = datetime.datetime.now()
            year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
            report_date_str = now.strftime("%Y%m%d")
            
        # Tạo đường dẫn thư mục phân vùng (Ví dụ: data/historical/2026/04/06)
        partition_path = os.path.join(HISTORICAL_DB_PATH, year, month, day)
        
        # Lệnh đảm bảo thư mục tồn tại, nếu chưa có thì tự động tạo mới (Recursively)
        os.makedirs(partition_path, exist_ok=True)
        
        # BƯỚC QUAN TRỌNG: Gắn thêm cột 'Report Date' vào dữ liệu trước khi lưu
        # Điều này giúp DuckDB sau này có thể lọc (Filter) và sắp xếp (Order) theo thời gian.
        # Đây là kỹ thuật giải quyết 'Tử huyệt 1' (Lặp dữ liệu) bằng cách đánh dấu thời gian.
        date_obj = datetime.datetime.strptime(report_date_str, "%Y%m%d").date()
        df = df.with_columns(pl.lit(date_obj).alias("Report Date"))
        
        # Đặt tên file theo chuẩn: SP_Report_NgàyThángNăm.parquet
        file_name = f"SP_Report_{report_date_str}.parquet"
        file_path = os.path.join(partition_path, file_name)
        
        # Thực hiện ghi file Parquet xuống đĩa cứng
        # compression='snappy': Chuẩn nén cân bằng nhất giữa Tốc độ và Dung lượng
        df.write_parquet(file_path, compression="snappy")
        
        print(f"✅ [STORAGE] Đã lưu thành công {df.height} dòng vào {file_path}")
        return True # Trả về thành công
        
    except Exception as e:
        # Nếu có lỗi (như hết bộ nhớ, không có quyền ghi file), thông báo lỗi
        print(f"❌ [STORAGE] Lỗi khi ghi vào kho Parquet: {e}")
        return False # Trả về thất bại

# Đoạn code dùng để kiểm tra tính năng lưu trữ độc lập
if __name__ == "__main__":
    # Giả lập một bảng dữ liệu nhỏ
    test_df = pl.DataFrame({
        "Campaign ID": ["123", "456"],
        "Clicks": [10, 20]
    })
    # Thử lưu vào ngày 05/04/2024
    save_to_historical(test_df, "20240405")
