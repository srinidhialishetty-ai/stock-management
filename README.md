# Inventory Platform

A production-style, role-driven inventory management and ordering system with report-scoped catalogs, analytics, and controlled admin approvals.

---

## Live Application

**Production URL**  
https://inventory-platform-glc1.onrender.com/

---

## Overview

Inventory Platform is designed to reflect real operational workflows found in inventory-centric businesses. It goes beyond basic CRUD by integrating:

- structured data ingestion from Excel
- report-scoped catalog sharing via tokens/QR
- transactional cart and checkout with stock validation
- role-based access control across multiple user layers
- owner-governed admin approval workflow
- analytics for decision support

The system is built to behave like an internal product used by teams, not a demo-only project.

---

## Core Capabilities

### Role-Based Access Model
- **Owner** — system-level control, approves admin access
- **Admin** — inventory ingestion, catalog publishing, analytics
- **Manager** — operational visibility and order monitoring
- **End User** — catalog browsing, cart, checkout
- **Guest** — limited preview access

Access is enforced at both route and UI levels.

---

### Inventory Ingestion
- Upload `.xlsx` files (real-world, flexible headers)
- Automatic dataset parsing and normalization
- Each upload forms a **report-scoped dataset**
- Edit, delete, and monitor stock per report

---

### Report-Scoped Catalog Sharing
- Generate **tokenized links / QR codes** per report
- Consumers see **only the scoped dataset**
- Clean separation between internal data and shared access

---

### Cart & Checkout
- Add items to cart without affecting stock
- Quantity updates and validation
- **Stock deducted only on confirmed checkout**
- Preserves transactional integrity

---

### Analytics & Insights
- Inventory value calculations
- Category distribution
- Low-stock identification
- Fast/slow-moving item detection
- Dashboard-driven insights using real data

---

### Admin Approval Workflow (Owner-Controlled)
- Users submit requests for admin access with proof
- Owners review and **approve/reject**
- Approved users continue admin onboarding
- Enforces controlled privilege escalation

---

### Authentication & Security
- Session-based authentication
- Owner-protected admin creation
- Password reset (token/OTP-style flow)
- Secure session signing via environment-managed secret key

---

## System Workflow

### Admin
1. Upload inventory (Excel)
2. Review parsed report
3. Publish catalog (token/QR)
4. Monitor analytics and stock

### End User
1. Open shared catalog (token/QR)
2. Browse items
3. Add to cart
4. Checkout (validated)
5. View order outcomes

### Owner
1. Log in via owner portal
2. Review admin requests
3. Approve or reject
4. Maintain system governance

---

## Technology Stack

**Frontend**
- HTML, CSS, JavaScript
- Chart.js

**Backend**
- Python, Flask

**Database**
- Oracle (primary)
- SQLite (fallback / demo)

**Data Processing**
- pandas, openpyxl

**Utilities**
- qrcode, Pillow

---

## Architecture Highlights

- **Report-scoped data isolation** for accurate analytics and sharing
- **Token-based access control** for catalogs
- **Session-driven authentication** with environment-managed secrets
- **Modular routing** for role separation
- **Hybrid DB fallback** for portability

---

## Running Locally

```bash
git clone https://github.com/<your-username>/stock-management.git
cd stock-management
pip install -r requirements.txt
python main.py
