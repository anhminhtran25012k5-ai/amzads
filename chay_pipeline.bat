@echo off
REM ===========================================================================
REM FILE CHAY_PIPELINE.BAT - KHỞI CHẠY HỆ THỐNG TỰ ĐỘNG HÓA AMAZON ADS
REM ===========================================================================
REM File này giúp người dùng chạy toàn bộ luồng công việc chỉ bằng 1 cú click.

REM 1. Di chuyển vào thư mục dự án
cd /d "f:\Minhpython\Test2"

REM 2. Kích hoạt môi trường ảo Python (venv) để đảm bảo có đủ thư viện
call "f:\Minhpython\venv\Scripts\activate"

REM 3. Chạy Pipeline chính: Xử lý file báo cáo mới -> Lưu vào DB -> Xuất Excel
echo [1/2] Đang chạy Pipeline xử lý dữ liệu...
python pipeline.py

REM 4. Thông báo hoàn tất và hỏi người dùng có muốn mở Dashboard không
echo.
echo ✅ Pipeline đã chạy xong! Kết quả nằm tại f:\Minhpython\Test2\data\output\Final_Analyzed_Report.xlsx
echo.

set /p user_choice="Ban co muon mo Dashboard (Streamlit) khong? (Y/N): "
if /i "%user_choice%"=="Y" (
    echo [2/2] Đang khởi động Dashboard...
    streamlit run dashboard.py
) else (
    echo Da ket thuc. Cam on ban!
    pause
)
