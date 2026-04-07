import os
import shutil
import re
import sys
from ingest_engine import load_sponsored_products
from storage_engine import save_to_historical

# Cấu hình đường dẫn
IMPORT_QUEUE = os.path.join("data", "import_queue")
PROCESSED_DIR = os.path.join("data", "processed")

# Đảm bảo console hiển thị đúng tiếng Việt
sys.stdout.reconfigure(encoding='utf-8')

def process_import_queue():
    """
    Quét thư mục import_queue, trích xuất ngày từ tên file và nạp vào DB.
    Trả về bộ giá trị: (số file đã nạp, đường dẫn file mới nhất trong processed).
    """
    # 1. Kiểm tra và tạo thư mục nếu chưa có
    os.makedirs(IMPORT_QUEUE, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # 2. Lấy danh sách các file excel trong queue
    files = [f for f in os.listdir(IMPORT_QUEUE) if f.endswith(".xlsx")]
    
    if not files:
        # Thay đổi giá trị trả về: (0, None)
        return 0, None

    print(f"🚀 [BULK] Tìm thấy {len(files)} tệp chờ xử lý trong queue...")
    processed_count = 0
    latest_date = "00000000"
    latest_file_path = None

    for filename in files:
        file_path = os.path.join(IMPORT_QUEUE, filename)
        
        # 3. Trích xuất ngày YYYYMMDD từ tên file (tìm chuỗi 8 chữ số đầu tiên)
        date_match = re.search(r"(\d{8})", filename)
        
        if date_match:
            report_date = date_match.group(1)
            print(f"\n📂 [BULK] Đang xử lý: {filename} (Ngày nhận diện: {report_date})")
        else:
            print(f"\n⚠️ [BULK] Không tìm thấy ngày YYYYMMDD trong tên file: {filename}. Bỏ qua.")
            continue

        # 4. Nạp dữ liệu
        df = load_sponsored_products(file_path)
        
        if df is not None:
            # 5. Nén và lưu vào kho Parquet
            success = save_to_historical(df, report_date)
            
            if success:
                # 6. Di chuyển file sang thư mục processed
                dest_path = os.path.join(PROCESSED_DIR, filename)
                
                # Nếu file đã tồn tại ở processed, thêm hậu tố để tránh ghi đè
                if os.path.exists(dest_path):
                    name, ext = os.path.splitext(filename)
                    dest_path = os.path.join(PROCESSED_DIR, f"{name}_dup_{os.urandom(2).hex()}{ext}")
                
                shutil.move(file_path, dest_path)
                print(f"✅ [BULK] Đã nén và di chuyển: {filename} -> data/processed")
                
                # 7. Cập nhật file mới nhất dựa trên ngày
                if report_date >= latest_date:
                    latest_date = report_date
                    latest_file_path = dest_path
                
                processed_count += 1
            else:
                print(f"❌ [BULK] Lỗi khi lưu trữ dữ liệu từ file: {filename}")
        else:
            print(f"❌ [BULK] Lỗi khi đọc file: {filename}")

    print(f"\n✨ [BULK] HOÀN TẤT: Đã xử lý thành công {processed_count}/{len(files)} tệp.")
    return processed_count, latest_file_path

if __name__ == "__main__":
    process_import_queue()
