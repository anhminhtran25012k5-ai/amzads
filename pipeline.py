import sys    # Thư viện hệ thống để can thiệp vào tiến trình chạy
import time   # Thư viện đo thời gian để tính tốc độ xử lý
import os     # Thư viện thao tác với đường dẫn file và thư mục

# Import các hàm nghiệp vụ từ các module "anh em" trong project
from ingest_engine import load_sponsored_products              # Hàm đọc Excel siêu tốc
from polars_executor import calculate_metrics_and_apply_rules  # Hàm chạy thuật toán lọc từ khóa/campaign
from output_engine import export_to_excel                     # Hàm xuất báo cáo Excel đa sheet
from storage_engine import save_to_historical                 # Hàm lưu trữ dữ liệu vào kho Parquet
from db_engine import DuckDBEngine                            # Engine DuckDB để truy vấn lịch sử
from bulk_import import process_import_queue                   # Module nạp dữ liệu hàng loạt [NEW]

# Cấu hình để Terminal trên Windows hiển thị đúng các ký tự Emoji và tiếng Việt (UTF-8)
sys.stdout.reconfigure(encoding='utf-8')

def main():
    """Hàm trung tâm điều phối toàn bộ luồng chạy của ứng dụng (Orchestrator)"""
    start_all = time.time()  # Ghi lại mốc thời gian lúc bắt đầu để tính tổng thời gian chạy
    
    # Định nghĩa đường dẫn file nguồn (Input) và file báo cáo (Output)
    fallback_input = os.path.join("data", "input", "report.xlsx")
    output_file = os.path.join("data", "output", "Final_Analyzed_Report.xlsx")
    
    # In ra dòng tiêu đề chào mừng hoành tráng
    print("=========================================================================")
    print(" 🚀 BẮT ĐẦU CHUỖI PIPELINE TỰ ĐỘNG HÓA AMAZON ADS (POLARS ENGINE 10X)")
    print("=========================================================================\n")
    
    # [BƯỚC 0]: Kiểm tra và nạp dữ liệu hàng loạt từ thư mục 'import_queue'
    print("⏳ [PIPELINE] Đang kiểm tra thư mục import_queue...")
    processed_count, latest_queue_file = process_import_queue()
    
    use_file = None
    already_saved = False

    if latest_queue_file:
        print(f"✅ [PIPELINE] Đã nạp thành công {processed_count} tệp từ queue.")
        print(f"🎯 [PIPELINE] Tự động chọn file Mới Nhất để phân tích: {os.path.basename(latest_queue_file)}")
        use_file = latest_queue_file
        already_saved = True # Vì bulk_import đã lưu vào kho Parquet rồi
    else:
        # Nếu queue trống, tìm file ở thư mục input cũ làm dự phòng
        if os.path.exists(fallback_input):
            print(f"ℹ️ [PIPELINE] Queue trống. Sử dụng file dự phòng tại: {fallback_input}")
            use_file = fallback_input
            already_saved = False # File này cần được nạp vào kho Parquet
        else:
            print("🛑 [PIPELINE] Không tìm thấy dữ liệu mới nào để phân tích (Queue & Input đều trống).")
            print("💡 Hãy thả file vào 'data/import_queue' và chạy lại.")
            return

    print("\n---------------------------------------------------------")

    # [BƯỚC 1]: Nạp dữ liệu
    df_raw = load_sponsored_products(use_file)
    if df_raw is None:
        print("🛑 Dừng tiến trình do lỗi đọc file.")
        return
    
    # [BƯỚC 1.2]: Lưu trữ file vào kho (nếu chưa nạp)
    if not already_saved:
        save_to_historical(df_raw)
    
    print("\n---------------------------------------------------------")
    
    # [BƯỚC 1.3]: Kết nối DuckDB để lấy dữ liệu lịch sử cho Excel Dashboard
    print("⏳ [DUCKDB] Đang tổng hợp dữ liệu lịch sử...")
    db = DuckDBEngine()
    db.create_unified_view()
    df_history       = db.get_historical_summary()           
    df_classification = db.get_keyword_campaign_classification() 
    detail_sheets    = db.get_classified_detail_sheets()       
    db.close()                             
    
    print("\n---------------------------------------------------------")
    
    # [BƯỚC 2]: Phân tích chuyên sâu (Polars)
    print("⏳ [POLARS] Đang áp dụng thuật toán phân tích...")
    df_analyzed, df_bad_isolated, df_good_isolated, df_superstars = calculate_metrics_and_apply_rules(df_raw)
    
    print("\n---------------------------------------------------------")
    
    # [BƯỚC 3]: Tập hợp kết quả và Xuất Excel
    print("⏳ [OUTPUT] Đang tạo file Excel báo cáo...")
    sheets_to_save = {
        "Analyzed Products"              : df_analyzed,        
        "CÁCH LY - Campaign Lỗi"        : df_bad_isolated,    
        "CHĂM SÓC DÀI HẠN - Camp Tốt"  : df_good_isolated,   
        "💎 TỪ KHÓA SIÊU SAO (Vít Bid)"  : df_superstars,      
    }

    if detail_sheets:
        if detail_sheets.get("camp_good") is not None and detail_sheets["camp_good"].height > 0:
            sheets_to_save["🟢 Campaign Tốt (No KW Yếu)"] = detail_sheets["camp_good"]
        if detail_sheets.get("camp_weak") is not None and detail_sheets["camp_weak"].height > 0:
            sheets_to_save["🔴 Campaign Yếu (No KW Khỏe)"] = detail_sheets["camp_weak"]
        if detail_sheets.get("kw_strong") is not None and detail_sheets["kw_strong"].height > 0:
            sheets_to_save["✨ TỪ Khóa Khỏe"]              = detail_sheets["kw_strong"]
        if detail_sheets.get("kw_weak") is not None and detail_sheets["kw_weak"].height > 0:
            sheets_to_save["⚠️ TỪ Khóa Yếu"]               = detail_sheets["kw_weak"]

    export_to_excel(sheets_to_save, output_file, df_history=df_history, df_classification=df_classification)
    
    # Kết thúc
    elapsed = time.time() - start_all
    print(f"\n✨ TOÀN BỘ TIẾN TRÌNH HOÀN TẤT THÀNH CÔNG TRONG {elapsed:.3f} GIÂY! ✨")

# Khởi chạy
if __name__ == "__main__":
    main()
