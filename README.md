# Stock Management with Inventory Analysis

Production-style Flask web application for small and medium businesses that need reliable stock control, sales tracking, flexible product input methods, and business-friendly analytics on Oracle Database.

## Stack

- Frontend: HTML, CSS, JavaScript
- Backend: Flask
- Database: Oracle Database via `oracledb`
- Excel import: `pandas` + `openpyxl`
- QR generation: `qrcode` + `Pillow`
- Analytics: built-in business dashboard

## Features

- Premium landing page with strong nude SaaS styling
- Session-based authentication with duplicate username prevention
- Optional, non-breaking Google login architecture
- Inventory CRUD with search, category filtering, and low-stock highlighting
- Sales recording with atomic stock deduction
- Excel-based bulk product import
- OCR-ready image upload pipeline
- QR generation and scan-payload lookup flow
- Analytics dashboard with fast-moving, slow-moving, low-stock, and category insights
- Business-friendly decision support recommendations

## Project structure

```text
D:\stockManagement
|-- main.py
|-- db.py
|-- schema.sql
|-- requirements.txt
|-- README.md
|-- templates\
|-- static\
|   |-- style.css
|   |-- app.js
|   |-- uploads\
|   `-- qr\
|-- utils\
|   |-- analytics_utils.py
|   |-- excel_import.py
|   `-- qr_utils.py
`-- sample_data\
```

## Oracle configuration

Default database settings are already wired into `db.py`:

- User: `rfp`
- Password: `rfp123`
- DSN: `localhost:1521/XEPDB1`

You can override them with environment variables:

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Make sure Oracle XE / your Oracle instance is running and the `rfp` schema exists.
4. Start the app:

```powershell
python main.py
```

5. Open [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Schema and seed data

- Run [`schema.sql`](/D:/stockManagement/schema.sql) manually in Oracle if you want full control.
- The app also attempts to initialize the schema on startup.
- Seed inventory rows are included in the schema for a quick demo.

## Optional Google login architecture

The manual login path is the stable default. Google login is intentionally non-breaking and can be enabled later with:

- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REDIRECT_URI`

## Excel template

Use the sample file in [`sample_data/products_template.xlsx`](/D:/stockManagement/sample_data/products_template.xlsx) or the CSV companion in [`sample_data/products_template.csv`](/D:/stockManagement/sample_data/products_template.csv).

Required columns:

- `name`
- `category`
- `price`
- `quantity`

## Demo flow

1. Open the landing page.
2. Register a user account.
3. Reach the protected dashboard.
4. Add products manually or upload the Excel template.
5. Record a sale.
6. Verify stock reduction in inventory.
7. Open analytics to review trends and recommendations.
8. Generate QR codes from the products page and test QR lookup with pasted payload data.

## Notes

- Database operations use parameterized queries.
- Query failures are handled with user-friendly messages.
- File uploads are validated for format and size.
- Image upload is OCR-ready without blocking the core application if OCR is unavailable.
