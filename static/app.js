const productSelect = document.getElementById("sale-product-select");
const quantityInput = document.getElementById("sale-quantity-input");
const totalPreview = document.getElementById("sale-total-preview");
const stockPreview = document.getElementById("sale-stock-preview");
const themeToggle = document.getElementById("theme-toggle");
const themeToggleLabel = document.getElementById("theme-toggle-label");
const loginStage = document.querySelector("[data-login-page]");

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

if (loginStage) {
    const panel = document.getElementById("login-panel");
    const roleButtons = Array.from(document.querySelectorAll(".login-role-card"));
    const roleIntent = document.getElementById("role-intent");
    const roleTitle = document.getElementById("login-role-title");
    const rolePill = document.getElementById("login-role-pill");
    const formHeading = document.getElementById("login-form-heading");
    const formSubtitle = document.getElementById("login-form-subtitle");
    const loginSubmit = document.getElementById("login-submit");
    const credentialForm = document.getElementById("credential-form");
    const guestPanel = document.getElementById("guest-panel");
    const usernameInput = document.getElementById("login-username");
    const passwordInput = document.getElementById("login-password");
    const cursor = document.querySelector(".login-cursor");
    const cursorTrail = document.querySelector(".login-cursor-trail");
    const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    const roleContent = {
        admin: {
            title: "Admin Access",
            pill: "ADMIN",
            heading: "Authenticate into the management layer.",
            subtitle: "Upload stock, manage reports, publish shared catalogs, and monitor the full business surface.",
            button: "Enter Admin Workspace",
        },
        manager: {
            title: "Manager Access",
            pill: "MANAGER",
            heading: "Enter the operations command layer.",
            subtitle: "Track orders, monitor analytics, and keep fulfillment moving with a live operations view.",
            button: "Open Manager Workspace",
        },
        user: {
            title: "User Access",
            pill: "USER",
            heading: "Open the shared ordering experience.",
            subtitle: "Browse shared catalogs, build a cart, and check out with live stock validation.",
            button: "Enter User Portal",
        },
        guest: {
            title: "Guest Access",
            pill: "GUEST",
            heading: "Preview the platform without credentials.",
            subtitle: "Explore the shared product experience in read-only mode before switching into a full account.",
            button: "Continue as Guest",
        },
    };

    function activateRole(role) {
        const config = roleContent[role] || roleContent.admin;
        loginStage.dataset.activeRole = role;
        roleButtons.forEach((button) => {
            button.classList.toggle("is-active", button.dataset.role === role);
        });
        roleIntent.value = role;
        roleTitle.textContent = config.title;
        rolePill.textContent = config.pill;
        formHeading.textContent = config.heading;
        formSubtitle.textContent = config.subtitle;
        loginSubmit.textContent = config.button;

        const isGuest = role === "guest";
        credentialForm.hidden = isGuest;
        guestPanel.hidden = !isGuest;
        if (usernameInput) {
            usernameInput.required = !isGuest;
        }
        if (passwordInput) {
            passwordInput.required = !isGuest;
        }

        panel.classList.remove("role-shift");
        void panel.offsetWidth;
        panel.classList.add("role-shift");
    }

    roleButtons.forEach((button) => {
        button.addEventListener("click", () => activateRole(button.dataset.role));
    });

    const magneticTargets = Array.from(document.querySelectorAll(".magnetic"));
    magneticTargets.forEach((target) => {
        target.addEventListener("mousemove", (event) => {
            if (prefersReducedMotion) return;
            const rect = target.getBoundingClientRect();
            const offsetX = ((event.clientX - rect.left) / rect.width - 0.5) * 10;
            const offsetY = ((event.clientY - rect.top) / rect.height - 0.5) * 10;
            target.style.transform = `translate(${offsetX}px, ${offsetY}px) scale(1.01)`;
        });
        target.addEventListener("mouseleave", () => {
            target.style.transform = "";
        });
    });

    if (!prefersReducedMotion) {
        const depthNodes = Array.from(document.querySelectorAll("[data-depth]"));
        window.addEventListener("mousemove", (event) => {
            const x = event.clientX / window.innerWidth - 0.5;
            const y = event.clientY / window.innerHeight - 0.5;

            depthNodes.forEach((node) => {
                const depth = Number(node.dataset.depth || 0);
                node.style.transform = `translate3d(${x * depth}px, ${y * depth}px, 0)`;
            });

            if (panel) {
                panel.style.setProperty("--tilt-x", `${y * -7}deg`);
                panel.style.setProperty("--tilt-y", `${x * 9}deg`);
            }
        });
    }

    if (cursor && cursorTrail && window.matchMedia("(pointer: fine)").matches) {
        let currentX = window.innerWidth / 2;
        let currentY = window.innerHeight / 2;
        let trailX = currentX;
        let trailY = currentY;

        window.addEventListener("mousemove", (event) => {
            currentX = event.clientX;
            currentY = event.clientY;
            cursor.style.opacity = "1";
            cursorTrail.style.opacity = "1";
        });

        const hoverables = document.querySelectorAll("a, button, input, select");
        hoverables.forEach((node) => {
            node.addEventListener("mouseenter", () => document.body.classList.add("cursor-active"));
            node.addEventListener("mouseleave", () => document.body.classList.remove("cursor-active"));
        });

        function animateCursor() {
            trailX += (currentX - trailX) * 0.14;
            trailY += (currentY - trailY) * 0.14;
            cursor.style.transform = `translate(${currentX}px, ${currentY}px)`;
            cursorTrail.style.transform = `translate(${trailX}px, ${trailY}px)`;
            requestAnimationFrame(animateCursor);
        }
        animateCursor();
    }

    requestAnimationFrame(() => {
        loginStage.classList.add("login-stage-ready");
    });

    activateRole("admin");
}
