import polars as pl               # Thư viện xử lý bảng dữ liệu (DataFrame) cực nhanh bằng Rust
import time                        # Thư viện đo hiệu năng thời gian
import os                          # Thư viện thao tác với tệp tin và đường dẫn
import sys                         # Thư viện hệ thống

# Import hàm chuẩn hóa cấu trúc từ file pandera_schema.py
from pandera_schema import reconcile_schema

# Đảm bảo console Windows in được các biểu tượng Emoji và tiếng Việt
sys.stdout.reconfigure(encoding='utf-8')

def load_sponsored_products(filepath: str) -> pl.DataFrame:
    """
    Hàm chủ chốt để đọc dữ liệu từ báo cáo quảng cáo Amazon (Sponsored Products).
    Sử dụng Calamine Engine để đạt tốc độ tối đa (nhanh hơn các thư viện truyền thống 10-20 lần).
    """
    print(f"⏳ [INGEST] Bắt đầu quét file: {filepath}")
    start_time = time.time()  # Bắt đầu bấm giờ
    
    try:
        # Sử dụng Polars để đọc file Excel trực tiếp
        # sheet_name: Tên sheet chứa dữ liệu quảng cáo của Amazon
        # engine: 'calamine' là engine mạnh nhất hiện nay cho việc đọc Excel bằng Rust
        df_raw = pl.read_excel(
            filepath,
            sheet_name="Sponsored Products Campaigns",
            engine="calamine"
        )
        
        # --- BƯỚC QUAN TRỌNG: BẢO VỆ CÁC CỘT VÀ MÃ ID ---
        # Gọi hàm reconcile_schema để:
        # 1. Tự động thêm các cột bị thiếu (nếu Amazon thay đổi mẫu file).
        # 2. Ép kiểu các ID (như Campaign Id, Keyword Id) về dạng CHUỖI.
        #    -> Điều này cực kỳ quan trọng để ngăn Excel tự ý biến ID thành số mũ (ví dụ 1.23E+10).
        df_safe = reconcile_schema(df_raw)
        
        # Tính toán thời gian đã trôi qua
        elapsed = time.time() - start_time
        print(f"✅ Đọc và đối chiếu cấu trúc thành công! Tổng số: {df_safe.height} dòng, {df_safe.width} cột.")
        print(f"🚀 Tốc độ tải xong: {elapsed:.3f} giây.")
        
        return df_safe  # Trả về DataFrame "Sạch" đã qua kiểm duyệt
        
    except Exception as e:
        # Nếu có lỗi (file bị mở bởi app khác hoặc đường dẫn sai), in ra lỗi và trả về None
        print(f"❌ Lỗi khi đọc file Input: {e}")
        return None

# Đoạn code dùng để chạy thử khi lập trình viên muốn kiểm tra riêng file này (Unit Test)
if __name__ == "__main__":
    input_file = "data/input/report.xlsx"  # Đường dẫn file test
    
    if os.path.exists(input_file):
        df_sponsored = load_sponsored_products(input_file)
        
        if df_sponsored is not None:
             print("\n📊 Kiểm tra mẫu ngẫu nhiên 3 dòng đầu (Hiển thị các cột ID):")
             # Lọc ra các cột là ID để kiểm tra kiểu dữ liệu
             id_cols = [c for c in df_sponsored.columns if c.lower().endswith(" id") or c.lower() == "id"]
             print(df_sponsored.select(id_cols).head(3))
    else:
        print(f"❌ Không tìm thấy file tại đường dẫn: {input_file}")
