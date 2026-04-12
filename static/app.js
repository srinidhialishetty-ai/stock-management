const productSelect = document.getElementById("sale-product-select");
const quantityInput = document.getElementById("sale-quantity-input");
const totalPreview = document.getElementById("sale-total-preview");
const stockPreview = document.getElementById("sale-stock-preview");
const themeToggle = document.getElementById("theme-toggle");
const themeToggleLabel = document.getElementById("theme-toggle-label");
const loginStage = document.querySelector("[data-login-page]");
const toastStack = document.getElementById("ui-toast-stack");
const footerYear = document.getElementById("footer-year");
const siteCursorDot = document.getElementById("site-cursor-dot");
const siteCursorHalo = document.getElementById("site-cursor-halo");

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

if (footerYear) {
    footerYear.textContent = new Date().getFullYear();
}

function showToast(message, tone = "info") {
    if (!toastStack || !message) {
        return;
    }
    const toast = document.createElement("div");
    toast.className = `ui-toast ui-toast-${tone}`;
    toast.textContent = message;
    toastStack.appendChild(toast);

    requestAnimationFrame(() => toast.classList.add("is-visible"));

    window.setTimeout(() => {
        toast.classList.remove("is-visible");
        window.setTimeout(() => toast.remove(), 380);
    }, 2400);
}

function setupGlobalMotion() {
    const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const finePointer = window.matchMedia("(pointer: fine)").matches;
    document.body.classList.add("page-ready");

    const revealTargets = Array.from(
        document.querySelectorAll(
            ".page-header, .hero-copy, .hero-panel, .metric-card, .feature-card, .dashboard-card, .showcase-card, .step-card, .standout-panel, .catalog-card, .cart-item, .order-card, .table-shell, .chart-card, .low-stock-card, .soft-empty, .contact-panel, .section-heading, .section-copy, .footer"
        )
    );

    revealTargets.forEach((node, index) => {
        node.classList.add("motion-reveal");
        node.style.setProperty("--motion-delay", `${Math.min(index * 36, 420)}ms`);
    });

    if (!prefersReducedMotion && "IntersectionObserver" in window) {
        const revealObserver = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting) {
                    entry.target.classList.add("motion-visible");
                    revealObserver.unobserve(entry.target);
                }
            });
        }, { threshold: 0.14, rootMargin: "0px 0px -8% 0px" });

        revealTargets.forEach((node) => revealObserver.observe(node));
    } else {
        revealTargets.forEach((node) => node.classList.add("motion-visible"));
    }

    const kpiNodes = Array.from(document.querySelectorAll(".dashboard-card h2, .metric-card strong"));
    const numberParser = /-?\d[\d,.]*/;
    const parseTarget = (text) => {
        const match = String(text).match(numberParser);
        if (!match) return null;
        return Number(match[0].replace(/,/g, ""));
    };

    const animateCounter = (node) => {
        if (node.dataset.counted === "true") return;
        const target = parseTarget(node.textContent);
        if (target === null || Number.isNaN(target)) return;
        node.dataset.counted = "true";
        const prefix = node.textContent.includes("Rs.") ? "Rs. " : "";
        const suffix = node.textContent.trim().endsWith("%") ? "%" : "";
        const decimals = String(node.textContent).includes(".") ? 2 : 0;
        const duration = 1050;
        const start = performance.now();

        function step(now) {
            const progress = Math.min((now - start) / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            const value = target * eased;
            const formatted = decimals
                ? value.toFixed(decimals)
                : Math.round(value).toLocaleString();
            node.textContent = `${prefix}${formatted}${suffix}`;
            if (progress < 1) {
                requestAnimationFrame(step);
            } else {
                const finalValue = decimals ? target.toFixed(decimals) : Math.round(target).toLocaleString();
                node.textContent = `${prefix}${finalValue}${suffix}`;
            }
        }
        requestAnimationFrame(step);
    };

    if (!prefersReducedMotion && "IntersectionObserver" in window) {
        const counterObserver = new IntersectionObserver((entries) => {
            entries.forEach((entry) => {
                if (entry.isIntersecting) {
                    animateCounter(entry.target);
                    counterObserver.unobserve(entry.target);
                }
            });
        }, { threshold: 0.6 });
        kpiNodes.forEach((node) => counterObserver.observe(node));
    } else {
        kpiNodes.forEach(animateCounter);
    }

    const interactiveSurfaces = Array.from(
        document.querySelectorAll(".hero-premium, .page-header, .feature-card, .dashboard-card, .catalog-card, .order-card, .cart-item, .mockup-window, .chart-shell, .topbar, .footer")
    );

    if (!prefersReducedMotion && finePointer) {
        const premiumCards = Array.from(document.querySelectorAll(".feature-card, .dashboard-card, .catalog-card, .order-card, .cart-item, .mockup-window, .chart-card"));
        premiumCards.forEach((card) => {
            card.addEventListener("mousemove", (event) => {
                const rect = card.getBoundingClientRect();
                const x = (event.clientX - rect.left) / rect.width - 0.5;
                const y = (event.clientY - rect.top) / rect.height - 0.5;
                card.style.transform = `perspective(1200px) rotateX(${y * -3.4}deg) rotateY(${x * 4.6}deg) translateY(-4px)`;
            });
            card.addEventListener("mouseleave", () => {
                card.style.transform = "";
            });
        });

        interactiveSurfaces.forEach((surface) => {
            surface.addEventListener("mousemove", (event) => {
                const rect = surface.getBoundingClientRect();
                const mouseX = event.clientX - rect.left;
                const mouseY = event.clientY - rect.top;
                surface.style.setProperty("--mx", `${mouseX}px`);
                surface.style.setProperty("--my", `${mouseY}px`);
                surface.classList.add("cursor-presence");
            });
            surface.addEventListener("mouseleave", () => {
                surface.classList.remove("cursor-presence");
            });
        });

        const depthNodes = Array.from(document.querySelectorAll(".hero-premium [data-depth], .page-header[data-depth], .motion-depth"));
        window.addEventListener("mousemove", (event) => {
            const x = event.clientX / window.innerWidth - 0.5;
            const y = event.clientY / window.innerHeight - 0.5;
            depthNodes.forEach((node) => {
                const depth = Number(node.dataset.depth || 0);
                node.style.transform = `translate3d(${x * depth}px, ${y * depth}px, 0)`;
            });
        });
    }

    const magneticTargets = Array.from(document.querySelectorAll(".btn, .nav-link, .theme-toggle, .action-tile"));
    magneticTargets.forEach((target) => {
        target.addEventListener("mousemove", (event) => {
            if (prefersReducedMotion || !finePointer) return;
            const rect = target.getBoundingClientRect();
            const offsetX = ((event.clientX - rect.left) / rect.width - 0.5) * 8;
            const offsetY = ((event.clientY - rect.top) / rect.height - 0.5) * 8;
            target.style.transform = `translate(${offsetX}px, ${offsetY}px)`;
        });
        target.addEventListener("mouseleave", () => {
            target.style.transform = "";
        });
    });

    const cartButtons = Array.from(document.querySelectorAll("form[action*='add-to-cart'] button, .btn[href*='checkout'], form button"));
    cartButtons.forEach((button) => {
        button.addEventListener("click", () => {
            if (button.disabled) return;
            const label = button.textContent.trim().toLowerCase();
            if (label.includes("add to cart")) {
                showToast("Added to cart. Review it before checkout.", "success");
                const cartLink = document.querySelector("[data-cart-link]");
                if (cartLink) {
                    cartLink.classList.add("cart-link-bounce");
                    window.setTimeout(() => cartLink.classList.remove("cart-link-bounce"), 700);
                }
            } else if (label.includes("proceed to checkout") || label.includes("go to checkout")) {
                showToast("Opening checkout with live stock validation.", "info");
            } else if (label.includes("confirm order")) {
                showToast("Confirming your order and updating live stock.", "success");
            }
            button.classList.add("action-pulse");
            window.setTimeout(() => button.classList.remove("action-pulse"), 460);
        });
    });

    document.querySelectorAll("[data-toast-message]").forEach((node) => {
        const tone = node.dataset.toastMessage || "info";
        showToast(node.textContent.trim(), tone);
    });

    if (!prefersReducedMotion && finePointer && siteCursorDot && siteCursorHalo) {
        document.body.classList.add("site-cursor-enabled");
        let cursorX = window.innerWidth / 2;
        let cursorY = window.innerHeight / 2;

        const interactiveNodes = document.querySelectorAll("a, button, input, select, textarea, .feature-card, .dashboard-card, .catalog-card, .chart-shell, .topbar");
        interactiveNodes.forEach((node) => {
            node.addEventListener("mouseenter", () => document.body.classList.add("site-cursor-hover"));
            node.addEventListener("mouseleave", () => document.body.classList.remove("site-cursor-hover"));
        });

        window.addEventListener("pointermove", (event) => {
            cursorX = event.clientX;
            cursorY = event.clientY;
            siteCursorDot.style.transform = `translate3d(${cursorX}px, ${cursorY}px, 0) translate(-50%, -50%)`;
            siteCursorHalo.style.transform = `translate3d(${cursorX}px, ${cursorY}px, 0) translate(-50%, -50%)`;
            document.body.classList.add("site-cursor-visible");
        });
    }

    const titleLines = Array.from(document.querySelectorAll(".hero-title > span"));
    titleLines.forEach((line, index) => {
        if (line.querySelector(".line-reveal")) return;
        const text = line.textContent;
        line.textContent = "";
        const inner = document.createElement("span");
        inner.className = "line-reveal";
        inner.style.setProperty("--line-delay", `${220 + index * 120}ms`);
        inner.textContent = text;
        line.appendChild(inner);
    });

    const headerTitles = Array.from(document.querySelectorAll(".chronicle-header h1"));
    headerTitles.forEach((heading) => {
        if (heading.dataset.motionReady === "true") return;
        heading.dataset.motionReady = "true";
        heading.classList.add("motion-reveal");
        heading.style.setProperty("--motion-delay", "120ms");
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

setupGlobalMotion();

if (loginStage) {
    document.body.classList.remove("login-cursor-enabled", "cursor-active");
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

    if (cursor) {
        cursor.style.display = "none";
        cursor.style.opacity = "0";
    }
    if (cursorTrail) {
        cursorTrail.style.display = "none";
        cursorTrail.style.opacity = "0";
    }

    requestAnimationFrame(() => {
        loginStage.classList.add("login-stage-ready");
    });

    activateRole("admin");
}
