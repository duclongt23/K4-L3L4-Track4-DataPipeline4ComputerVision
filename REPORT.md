# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Đức Long |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602917 |
| GitHub | duclongt23 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

2 nguồn telemetry có schema không đồng nhất.A dùng ts_utc theo ISO 8601, còn B dùng timestamp_ms theo Unix milliseconds; A dùng speed_mps (m/s), trong khi B dùng spd_kmh (km/h). Các cột hướng, tọa độ, gia tốc và điều khiển cũng có tên khác nhau.

Bronze lưu riêng mỗi nguồn để bảo toàn dữ liệu gốc và khả năng truy vết. Các trường được giữ ở dạng chuỗi nhằm tránh làm mất giá trị hoặc che giấu lỗi khi ép kiểu quá sớm. Việc chuẩn hóa schema và kiểm tra chất lượng được thực hiện ở Silver.

Nguồn A có 3.021 dòng, nguồn B có 3.234 dòng. Lịch sử Delta của bronze.raw_telemetry_a chỉ có version 0, thao tác WRITE với 3.021 dòng. Ingest lại không tạo thêm phiên bản, chứng minh tính idempotent.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline chuẩn hóa hai nguồn telemetry về một schema chung tại Silver: đồng nhất tên cột, chuyển tốc độ sang m/s, chuẩn hóa timestamp và trừ độ lệch đồng hồ 1,5 giây của drive B. Bằng chứng là telemetry B bắt đầu lúc 2025-10-08 14:30:00.009 UTC, gần khớp thời điểm video bắt đầu 14:30:00 UTC.

Tổng 6.255 dòng Bronze được phân thành 6.149 dòng hợp lệ và 106 dòng quarantine. Mỗi drive có 20 dòng trùng, 30 dòng thiếu tốc độ và 3 dòng tốc độ ngoài phạm vi hợp lệ, tổng cộng 53 dòng bị loại ở mỗi drive.

Các dòng lỗi được giữ trong bảng silver_telemetry_rejects thay vì xóa để phục vụ kiểm toán, truy vết nguyên nhân và điều chỉnh quy tắc làm sạch mà không làm mất dữ liệu gốc

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test max_gap_ms đặt mức warn vì khoảng mất tín hiệu telemetry là vấn đề chất lượng cần cảnh báo, nhưng không nhất thiết phải dừng toàn bộ pipeline. Ngược lại, in_range đặt mức error vì tốc độ vượt phạm vi hợp lệ có thể làm sai lệch dữ liệu và kết quả phân tích phía sau.

Trong thí nghiệm break-gate, khi nới luật từ 70 lên 1.000 m/s, dbt trả về PASS=5 WARN=1 ERROR=1. Test in_range_silver_telemetry_speed_mps__70__0 phát hiện 6 dòng vi phạm. Sau khi khôi phục luật gốc, kết quả trở lại PASS=6 WARN=1 ERROR=0.

Thí nghiệm chứng minh rằng quality gate có thể chặn dữ liệu không đạt điều kiện chất lượng ngay cả khi quy tắc làm sạch phía trước bị cấu hình sai.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
