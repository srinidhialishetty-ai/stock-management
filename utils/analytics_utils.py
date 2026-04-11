LOW_STOCK_THRESHOLD = 10
MIN_ROWS_FOR_AI_INSIGHTS = 3


def _normalize_entry_date(value):
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value) if value else "No date"


def build_dashboard_metrics(rows):
    total_products = len(rows)
    total_stock_units = sum(int(row.get("QUANTITY", 0) or 0) for row in rows)
    total_inventory_value = sum(
        float(row.get("PRICE", 0) or 0) * int(row.get("QUANTITY", 0) or 0) for row in rows
    )
    low_stock_list = [
        row for row in rows if int(row.get("QUANTITY", 0) or 0) <= LOW_STOCK_THRESHOLD
    ]

    category_totals = {}
    trend_points = {}
    for row in rows:
        category = row.get("CATEGORY", "Uncategorized")
        quantity = int(row.get("QUANTITY", 0) or 0)
        category_totals[category] = category_totals.get(category, 0) + quantity
        entry_key = _normalize_entry_date(row.get("ENTRY_DATE") or row.get("CREATED_AT"))
        trend_points[entry_key] = trend_points.get(entry_key, 0) + quantity

    sorted_categories = sorted(category_totals.items(), key=lambda item: item[1], reverse=True)
    return {
        "total_products": total_products,
        "total_stock_units": total_stock_units,
        "total_inventory_value": total_inventory_value,
        "low_stock_items": len(low_stock_list),
        "category_totals": category_totals,
        "trend_points": trend_points,
        "top_categories": sorted_categories[:5],
        "low_stock_list": low_stock_list[:5],
    }


def build_inventory_summary(rows):
    summary = {}
    for row in rows:
        category = row.get("CATEGORY", "Uncategorized")
        if category not in summary:
            summary[category] = {
                "CATEGORY": category,
                "TOTAL_PRODUCTS": 0,
                "TOTAL_UNITS": 0,
                "INVENTORY_VALUE": 0,
            }
        summary[category]["TOTAL_PRODUCTS"] += 1
        summary[category]["TOTAL_UNITS"] += int(row.get("QUANTITY", 0) or 0)
        summary[category]["INVENTORY_VALUE"] += float(row.get("PRICE", 0) or 0) * int(row.get("QUANTITY", 0) or 0)
    return sorted(summary.values(), key=lambda item: item["INVENTORY_VALUE"], reverse=True)


def build_insights(rows):
    metrics = build_dashboard_metrics(rows)
    messages = []
    if len(rows) < MIN_ROWS_FOR_AI_INSIGHTS:
        return {
            "enabled": False,
            "title": "AI insights unlock after more data",
            "messages": [
                "Upload at least three inventory rows to activate recommendation-style business insights."
            ],
        }

    if metrics["top_categories"]:
        top_category, units = metrics["top_categories"][0]
        messages.append(f"{top_category} is your strongest category right now with {units} units in stock.")
    if metrics["low_stock_list"]:
        item = metrics["low_stock_list"][0]
        messages.append(f"Low stock alert: {item['PRODUCT_NAME']} has only {int(item['QUANTITY'])} units left.")
    if rows:
        highest_value = max(rows, key=lambda row: float(row.get("PRICE", 0) or 0) * int(row.get("QUANTITY", 0) or 0))
        messages.append(
            f"{highest_value['PRODUCT_NAME']} carries one of the highest inventory values and should stay visible in promotions."
        )
    if not messages:
        messages.append("Upload inventory records to unlock role-based analytics and recommendations.")
    return {
        "enabled": True,
        "title": "AI business recommendations",
        "messages": messages,
    }


def build_chart_payload(rows):
    metrics = build_dashboard_metrics(rows)
    category_labels = list(metrics["category_totals"].keys())
    category_values = list(metrics["category_totals"].values())
    trend_labels = list(sorted(metrics["trend_points"].keys()))
    trend_values = [metrics["trend_points"][key] for key in trend_labels]
    return {
        "category_labels": category_labels,
        "category_values": category_values,
        "trend_labels": trend_labels,
        "trend_values": trend_values,
    }
