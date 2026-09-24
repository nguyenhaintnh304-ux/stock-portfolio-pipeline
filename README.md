# Stock Portfolio Pipeline

Tự động crawl giá CK VN hàng ngày lúc 15:30 (T2-T6), lưu vào Supabase,
tính PnL và cập nhật dashboard.

## Cấu trúc
- `pipeline.py` — code chính
- `requirements.txt` — thư viện
- `.github/workflows/daily_pipeline.yml` — GitHub Actions schedule

## Secrets cần tạo trên GitHub
Vào Settings → Secrets and variables → Actions → New repository secret:
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
