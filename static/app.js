const productSelect = document.getElementById("sale-product-select");
const quantityInput = document.getElementById("sale-quantity-input");
const totalPreview = document.getElementById("sale-total-preview");
const stockPreview = document.getElementById("sale-stock-preview");
const themeToggle = document.getElementById("theme-toggle");
const themeToggleLabel = document.getElementById("theme-toggle-label");

function applyTheme(theme) {
    const isNight = theme === "night";
    document.body.classList.toggle("theme-night", isNight);
    if (themeToggleLabel) {
        themeToggleLabel.textContent = isNight ? "Day Mode" : "Night Mode";
    }
    localStorage.setItem("stock-theme", isNight ? "night" : "day");
}

const savedTheme = localStorage.getItem("stock-theme");
if (savedTheme === "night") {
    applyTheme("night");
}

if (themeToggle) {
    themeToggle.addEventListener("click", () => {
        const isNight = document.body.classList.contains("theme-night");
        applyTheme(isNight ? "day" : "night");
    });
}

function updateSalePreview() {
    if (!productSelect || !quantityInput || !totalPreview || !stockPreview) {
        return;
    }

    const option = productSelect.options[productSelect.selectedIndex];
    const quantity = Number(quantityInput.value || 0);
    const price = Number(option?.dataset?.price || 0);
    const stock = Number(option?.dataset?.quantity || 0);
    const total = quantity > 0 ? quantity * price : 0;

    totalPreview.textContent = `Rs. ${total.toFixed(2)}`;
    if (!option || !option.value) {
        stockPreview.textContent = "Select a product to see stock availability.";
        return;
    }

    stockPreview.textContent = `${stock} units currently available.`;
    stockPreview.style.color = quantity > stock ? "#8b3f3f" : "#5C5C5C";
}

if (productSelect && quantityInput) {
    productSelect.addEventListener("change", updateSalePreview);
    quantityInput.addEventListener("input", updateSalePreview);
    updateSalePreview();
}
