"""
LOV3 Design System — Shared CSS, layout templates, and UI components.

All dashboards import from here to ensure consistent look and feel.
Light theme matching the Bank Review page style: white cards, soft gray
background, dark purple header gradient, indigo accent.
"""

# ─── Color Palette (Bank Review style — light professional theme) ───────────

COLORS = {
    "bg_base": "#f0f2f5",       # soft gray — page background
    "bg_card": "#ffffff",       # white — cards, sections
    "bg_nav": "#ffffff",        # white — nav bar
    "bg_hover": "#f3f4f6",     # light gray — hover states
    "bg_input": "#ffffff",      # white — form inputs
    "border": "#e5e7eb",        # light gray border
    "shadow": "0 1px 3px rgba(0,0,0,0.08)",  # subtle card shadow
    "text_primary": "#1f2937",  # dark gray — headings, values
    "text_secondary": "#6b7280", # medium gray — labels
    "text_muted": "#9ca3af",    # light gray — timestamps
    "accent": "#6366f1",        # indigo — active states, primary buttons
    "accent_light": "#818cf8",  # indigo light — hover
    "accent_bg": "#eef2ff",     # indigo tint — active nav pill
    "header_gradient": "linear-gradient(135deg, #0f0c29, #302b63, #24243e)",
    "success": "#27ae60",
    "warning": "#f59e0b",
    "danger": "#e74c3c",
    "info": "#2980b9",
}


def base_css() -> str:
    """Core CSS used by every dashboard — light theme matching Bank Review."""
    c = COLORS
    return f"""
/* ─── Reset & Typography ────────────────────────────────────────── */
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:{c['bg_base']};color:{c['text_primary']};font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;line-height:1.5;-webkit-font-smoothing:antialiased}}
a{{color:{c['accent']};text-decoration:none}}
a:hover{{color:{c['accent_light']}}}

/* ─── Navigation ────────────────────────────────────────────────── */
.nav-bar{{background:{c['bg_nav']};border-bottom:1px solid {c['border']};padding:8px 24px;display:flex;gap:6px;flex-wrap:wrap;position:sticky;top:0;z-index:100}}
.nav-bar a{{text-decoration:none;padding:7px 16px;border-radius:9999px;font-size:0.8rem;font-weight:600;color:{c['text_secondary']};transition:all 0.15s}}
.nav-bar a:hover{{background:{c['bg_hover']};color:{c['text_primary']}}}
.nav-bar a.active{{background:{c['accent']};color:#fff}}

/* ─── Layout ────────────────────────────────────────────────────── */
.container{{max-width:1280px;margin:0 auto;padding:24px}}
.header{{text-align:center;padding:28px 24px;background:{c['header_gradient']};border-radius:16px;margin-bottom:24px;color:#fff}}
.header h1{{font-size:1.5rem;font-weight:700;color:#fff;letter-spacing:-0.02em}}
.header p,.header .subtitle{{color:rgba(255,255,255,0.7);font-size:0.9rem;margin-top:6px}}
.section{{background:{c['bg_card']};border-radius:12px;padding:20px;margin-bottom:16px;box-shadow:{c['shadow']};border:1px solid {c['border']}}}
.section h2{{font-size:1rem;font-weight:600;color:{c['text_primary']};margin-bottom:14px}}

/* ─── KPI Cards ─────────────────────────────────────────────────── */
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}}
.kpi{{background:{c['bg_card']};border-radius:12px;padding:20px;text-align:center;box-shadow:{c['shadow']};border:1px solid {c['border']};transition:transform 0.15s,box-shadow 0.15s}}
.kpi:hover{{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,0.1)}}
.kpi .label{{font-size:0.72rem;color:{c['text_secondary']};text-transform:uppercase;letter-spacing:0.06em;font-weight:500}}
.kpi .value{{font-size:1.7rem;font-weight:700;margin:8px 0 4px;color:{c['text_primary']}}}
.kpi .change{{font-size:0.8rem;font-weight:600}}
.kpi .change.up{{color:{c['success']}}}
.kpi .change.down{{color:{c['danger']}}}
.kpi .change.neutral{{color:{c['text_secondary']}}}

/* ─── Tables ────────────────────────────────────────────────────── */
table{{width:100%;border-collapse:collapse;font-size:0.85rem}}
th{{text-align:left;color:{c['text_secondary']};padding:10px 12px;border-bottom:2px solid {c['border']};font-weight:500;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.04em}}
td{{padding:10px 12px;border-bottom:1px solid {c['border']}}}
tr:hover{{background:{c['bg_hover']}}}
tbody tr:last-child td{{border-bottom:none}}

/* ─── Buttons ───────────────────────────────────────────────────── */
.btn{{padding:8px 20px;border-radius:8px;font-weight:600;font-size:0.85rem;cursor:pointer;border:none;transition:all 0.15s}}
.btn-primary{{background:{c['accent']};color:#fff}}.btn-primary:hover{{background:{c['accent_light']}}}
.btn-success{{background:{c['success']};color:#fff}}.btn-success:hover{{opacity:0.9}}
.btn-danger{{background:{c['danger']};color:#fff}}.btn-danger:hover{{opacity:0.9}}
.btn-ghost{{background:transparent;color:{c['text_secondary']};border:1px solid {c['border']}}}.btn-ghost:hover{{color:{c['text_primary']};border-color:{c['text_secondary']}}}

/* ─── Form Inputs ───────────────────────────────────────────────── */
input[type="text"],input[type="date"],input[type="number"],select,textarea{{
  background:{c['bg_input']};border:1px solid {c['border']};color:{c['text_primary']};
  padding:8px 14px;border-radius:8px;font-size:0.9rem;transition:border-color 0.15s,box-shadow 0.15s;width:auto}}
input:focus,select:focus,textarea:focus{{outline:none;border-color:{c['accent']};box-shadow:0 0 0 3px {c['accent']}22}}

/* ─── Badges & Status ───────────────────────────────────────────── */
.badge{{display:inline-block;padding:2px 10px;border-radius:9999px;font-size:0.7rem;font-weight:600}}
.badge-success{{background:{c['success']}18;color:{c['success']}}}
.badge-warning{{background:{c['warning']}18;color:{c['warning']}}}
.badge-danger{{background:{c['danger']}18;color:{c['danger']}}}
.badge-info{{background:{c['info']}18;color:{c['info']}}}

/* ─── Progress / Bars ───────────────────────────────────────────── */
.bar-track{{height:6px;background:{c['border']};border-radius:3px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:3px;transition:width 0.3s ease}}
.bar-fill.success{{background:{c['success']}}}.bar-fill.warning{{background:{c['warning']}}}.bar-fill.danger{{background:{c['danger']}}}.bar-fill.accent{{background:{c['accent']}}}

/* ─── Filter / Date Bar ─────────────────────────────────────────── */
.filter-bar{{display:flex;justify-content:center;gap:12px;margin:16px 0;flex-wrap:wrap;align-items:center}}

/* ─── Loading / Error States ────────────────────────────────────── */
.loading{{text-align:center;padding:40px;color:{c['text_secondary']}}}
.error-msg{{text-align:center;padding:24px;color:{c['danger']};background:{c['danger']}11;border-radius:8px}}

/* ─── Alerts ────────────────────────────────────────────────────── */
.alert{{border-radius:8px;padding:12px 16px;margin-bottom:8px;border:1px solid}}
.alert-danger{{background:{c['danger']}08;border-color:{c['danger']}33;color:{c['danger']}}}
.alert-warning{{background:{c['warning']}08;border-color:{c['warning']}33;color:#92400e}}
.alert-success{{background:{c['success']}08;border-color:{c['success']}33;color:#166534}}

/* ─── Responsive ────────────────────────────────────────────────── */
@media(max-width:768px){{
  .container{{padding:12px}}
  .header{{padding:16px}}
  .kpi-grid{{grid-template-columns:1fr 1fr}}
  .filter-bar{{flex-direction:column;align-items:stretch}}
  .nav-bar{{padding:4px 12px;gap:4px}}
  .nav-bar a{{padding:6px 12px;font-size:0.72rem}}
}}
@media(max-width:480px){{
  .kpi-grid{{grid-template-columns:1fr}}
  .kpi .value{{font-size:1.3rem}}
}}
"""


def page_shell(title: str, active_path: str, body_html: str, extra_css: str = "", extra_js: str = "") -> str:
    """Wrap dashboard content in the standard page shell.

    Args:
        title: Page title (shown in browser tab)
        active_path: URL path for nav bar active highlight
        body_html: The dashboard's main content HTML
        extra_css: Additional CSS specific to this dashboard
        extra_js: JavaScript for this dashboard
    """
    from dashboards import _nav_html

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
{base_css()}
{extra_css}
</style>
</head>
<body>
{_nav_html(active_path)}
<div class="container">
{body_html}
</div>
{f'<script>{extra_js}</script>' if extra_js else ''}
</body>
</html>"""
