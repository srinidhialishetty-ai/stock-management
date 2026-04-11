from db import safe_execute


REQUIRED_COLUMNS = {"name", "category", "price", "quantity"}


def import_products_from_excel(file_path):
    try:
        import pandas as pd

        dataframe = pd.read_excel(file_path)
    except Exception:
        return False, "We could not read that Excel file. Please upload a valid .xlsx template.", {}

    normalized_columns = {str(column).strip().lower(): column for column in dataframe.columns}
    if not REQUIRED_COLUMNS.issubset(set(normalized_columns.keys())):
        return False, "Invalid columns. Your Excel file must include name, category, price, and quantity.", {}

    inserted = 0
    skipped = 0
    for _, row in dataframe.iterrows():
        try:
            name = str(row[normalized_columns["name"]]).strip()
            category = str(row[normalized_columns["category"]]).strip()
            price = float(row[normalized_columns["price"]])
            quantity = int(row[normalized_columns["quantity"]])
            if not name or not category or price < 0 or quantity < 0:
                skipped += 1
                continue

            success, _message, _ = safe_execute(
                """
                INSERT INTO products (name, category, price, quantity)
                VALUES (:name, :category, :price, :quantity)
                """,
                {"name": name, "category": category, "price": price, "quantity": quantity},
            )
            if success:
                inserted += 1
            else:
                skipped += 1
        except Exception:
            skipped += 1

    return True, "Excel import completed.", {"inserted": inserted, "skipped": skipped}
