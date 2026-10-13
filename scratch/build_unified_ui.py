import re

with open('client/laptop_server_d_m_light.py.broken_light_bak', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Python pre-HTML and post-HTML
html_marker = 'HTML_TEMPLATE = r"""'
pre_html = text[:text.find(html_marker)]
post_html = text[text.find('def main():'):]

# Extract entire HTML
idx_html_start = text.find(html_marker) + len(html_marker)
idx_html_end = text.rfind('"""', 0, text.find('def main():'))
html = text[idx_html_start:idx_html_end].strip()

# Set dark theme on html tag
html = html.replace('<html lang="en">', '<html lang="en" data-theme="dark">')

# 2. Extract CSS tokens and update for Image 1 dark theme & max-width container
# Add responsive container and refined styling
style_inject = """
.app-container {
  max-width: 1380px;
  margin: 0 auto;
  width: 100%;
}

.panel-title {
  font-family: var(--font-ui) !important;
  font-size: .80rem !important;
  font-weight: 600 !important;
  letter-spacing: .02em !important;
  text-transform: none !important;
  color: var(--text) !important;
  margin-bottom: 10px;
  display: flex;
  align-items: center;
  gap: 6px;
}

.panel-title .bar {
  color: var(--accent2);
  font-weight: 700;
  font-size: 0.95rem;
  line-height: 1;
  display: inline-block;
  margin-right: 2px;
}

.gcs-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 18px 4px 10px;
  margin-top: 18px;
  font-family: var(--font-ui);
  font-size: 0.70rem;
  color: #4a5d72;
  border-top: 1px solid var(--border);
  letter-spacing: 0.03em;
}

.link-subtabs {
  display: flex;
  gap: 4px;
  background: var(--bg2);
  padding: 3px;
  border-radius: var(--radius-sm);
  border: 1px solid var(--border);
  margin: 8px 0;
}
.link-subtab {
  flex: 1;
  padding: 6px 12px;
  border: none;
  border-radius: var(--radius-sm);
  background: transparent;
  color: var(--muted);
  font-family: var(--font-ui);
  font-size: .74rem;
  font-weight: 600;
  cursor: pointer;
  transition: all .15s ease;
}
.link-subtab.active {
  background: #22d3ee !important;
  color: #04121a !important;
  font-weight: 700 !important;
  border-color: #22d3ee !important;
}

.btn-danger {
  background: #3b1419;
  border: 1px solid #7f1d1d;
  color: #f87171;
}
.btn-danger:hover {
  background: #4c1d24;
  border-color: #991b1b;
  color: #fca5a5;
}

.btn-ok {
  background: #064e3b;
  border: 1px solid #059669;
  color: #34d399;
}
.btn-ok:hover {
  background: #047857;
  border-color: #10b981;
  color: #6ee7b7;
}

.console-box {
  background: var(--console-bg) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-sm);
  padding: 10px 12px;
  font-family: var(--font-mono);
  font-size: .75rem;
  color: var(--console-text);
  overflow-y: auto;
}
"""

html = html.replace('</style>', style_inject + '\n</style>')

# Wrap body in .app-container
body_idx = html.find('<body>')
offline_banner_marker = '<div id="gcsOfflineBanner"'
if offline_banner_marker in html:
    banner_end = html.find('</div>', html.find(offline_banner_marker)) + 6
    html = html[:banner_end] + '\n<div class="app-container">\n' + html[banner_end:]
    # close app-container before </body>
    body_close = html.rfind('</body>')
    html = html[:body_close] + '\n</div>\n' + html[body_close:]
else:
    html = html.replace('<body>', '<body>\n<div class="app-container">\n')
    body_close = html.rfind('</body>')
    html = html[:body_close] + '\n</div>\n' + html[body_close:]

# 3. Restructure panels:
# In broken_light_bak:
# - Telemetry panel: lines 4725-4843
# - Commands panel: lines 4844-4925
# - Column 3 has Card 3A (Link Channels) and Card 3B (File Storage)
# - grid-bottom has Panel 1 (Physical Joy), Panel 2 (Virtual Joy), Panel 3 (Compass)
# - Wifi comms
# - Diagnostic Row 1 (Topics, Black box)
# - Diagnostic Row 2 (Params, Bad data)
# - Remote Terminal

# We want:
# In grid-main:
#   Column 1: Telemetry
#   Column 2: Commands
#   Column 3: File Storage (Card 3B)
# In grid-bottom (Row 1):
#   Column 1: Physical joystick
#   Column 2: Virtual joystick
#   Column 3: Link Channels (Card 3A)
# Row 2: Wifi comms
# Row 3: ROS2 Topics | Black Box Console
# Row 4: ROS2 Parameters | Compass (Panel 3)
# Row 5: Bad Data | Remote Terminal
# Footer: AUV Ground Control Module | Tethered Vehicle Operations

# Let's extract Card 3A (Link Channels) and Card 3B (File Storage):
card_3a_start = html.find('<!-- Card 3A: Tether Link Channels -->')
card_3b_start = html.find('<!-- Card 3B: File Storage // Jetson System -->')
col_3_end = html.find('</div><!-- /Column 3 -->')

card_3a_html = html[card_3a_start:card_3b_start].strip()
card_3b_html = html[card_3b_start:col_3_end].strip()

# Extract Panel 3 (Compass):
compass_start = html.find('<!-- === Panel 3: Navigational Heading Compass === -->')
grid_bottom_end = html.find('</div><!-- /.grid-bottom -->')
compass_html = html[compass_start:grid_bottom_end].strip()

# In Column 3 of grid-main: put ONLY File Storage:
new_col_3 = f"""  <!-- === File Storage (Column 3) === -->
  <div class="panel">
    {card_3b_html}
  </div>"""

# Replace Column 3 of grid-main:
col_3_block_start = html.find('<!-- === Communications & File Storage (Column 3) === -->')
html = html[:col_3_block_start] + new_col_3 + '\n</div><!-- /.grid-main -->\n' + html[col_3_end + len('</div><!-- /Column 3 -->\n</div><!-- /.grid-main -->'):]

# In grid-bottom (Row 1): Panel 1 (Physical Joystick), Panel 2 (Virtual Joystick), Panel 3 (Link Channels):
# Replace Compass in grid-bottom with Link Channels:
new_link_channels_panel = f"""  <!-- === Panel 3: Link Channels === -->
  <div class="panel">
    {card_3a_html}
  </div>"""

# Find compass in grid-bottom and replace with link channels
c_idx = html.find('<!-- === Panel 3: Navigational Heading Compass === -->')
gb_end = html.find('</div><!-- /.grid-bottom -->')
html = html[:c_idx] + new_link_channels_panel + '\n' + html[gb_end:]

# Now let's handle Row 3, 4, 5:
# Currently in html:
# Wifi Comms
# Diagnostic Supervisor Row 1 (Topics & Black Box)
# Diagnostic Supervisor Row 2 (Params & Bad Data)
# Remote Terminal

# We want:
# Row 3: Topics & Black Box
# Row 4: Parameters & Compass
# Row 5: Bad Data & Remote Terminal
# Footer: AUV Ground Control Module | Tethered Vehicle Operations

diag_row_2_start = html.find('<!-- === Diagnostic Supervisor Row 2: Parameters & Packet Integrity === -->')
term_start = html.find('<!-- === Remote Terminal === -->')
reboot_modal_start = html.find('<!-- === Reboot sudo-password modal === -->')

# Extract Params panel and Bad Data panel
params_panel_start = html.find('<!-- === ROS2 Dynamic Parameters === -->', diag_row_2_start)
bad_data_start = html.find('<!-- === Link Integrity & Packet Checksum Diagnostics === -->', diag_row_2_start)

params_html = html[params_panel_start:bad_data_start].strip()
bad_data_html = html[bad_data_start:term_start].strip()
# Remove closing </div> of Row 2 from bad_data_html if present
if bad_data_html.endswith('</div>'):
    bad_data_html = bad_data_html[:-6].strip()

# Extract Remote Terminal
term_html = html[term_start:reboot_modal_start].strip()

# Assemble new Row 4, Row 5, and Footer:
new_rows_4_5_footer = f"""<!-- === Diagnostic Row 2: Parameters & Compass (2 columns) === -->
<div class="grid-tools" style="margin-top:14px;">

  <!-- === ROS2 Parameters === -->
  {params_html}

  <!-- === Compass === -->
  <div class="panel">
    {compass_html}
  </div>

</div>

<!-- === Diagnostic Row 3: Bad Data & Remote Terminal (2 columns) === -->
<div class="grid-tools" style="margin-top:14px;">

  <!-- === Bad Data === -->
  {bad_data_html}

  <!-- === Remote Terminal === -->
  {term_html}

</div>

<!-- === GCS Footer === -->
<footer class="gcs-footer">
  <div>AUV Ground Control Module</div>
  <div>Tethered Vehicle Operations</div>
</footer>
"""

html = html[:diag_row_2_start] + new_rows_4_5_footer + '\n' + html[reboot_modal_start:]

# 4. Update panel titles to have <span class="bar">|</span> and clean title-case text
title_replacements = [
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Physical Joystick\s*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Physical joystick</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Virtual Joystick[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Virtual joystick &nbsp;<span style="color:var(--muted);font-size:.7rem;font-weight:normal;">emergency</span></div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Tether Link Channels\s*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Link Channels</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*WLAN Comms[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Wifi Comms</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Wifi Comms[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Wifi Comms</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*ROS2 Topics\s*</div>',
     r'<div class="panel-title"><span class="bar">|</span> ROS2 Topics &nbsp;<span class="pill" style="font-size:0.58rem;padding:1px 6px;color:var(--accent2);border-color:var(--border2);background:var(--bg2);">LIVE GRAPH, JETSON SIDE</span></div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Flight Recorder &amp; Black Box Console\s*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Black Box Console</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*ROS2 Dynamic Parameters\s*</div>',
     r'<div class="panel-title"><span class="bar">|</span> ROS2 Parameters</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Navigational Heading Compass\s*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Compass</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Link Integrity[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Bad Data</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Onboard Remote Terminal[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Remote Terminal &nbsp;<span class="pill" style="font-size:0.58rem;padding:1px 6px;color:var(--ok);border-color:var(--ok-dim);background:var(--bg2);">WS OK</span></div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Telemetry[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Telemetry</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*Commands[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> Commands</div>'),
    (r'<div class="panel-title"><span class="dot"[^>]*></span>\s*File Storage[^<]*</div>',
     r'<div class="panel-title"><span class="bar">|</span> File Storage</div>'),
]

for pat, repl in title_replacements:
    html = re.sub(pat, repl, html)

# Also ensure default theme is dark in JS
html = html.replace(
    "applyTheme(localStorage.getItem('gcsTheme') || 'light');",
    "applyTheme(localStorage.getItem('gcsTheme') || 'dark');"
)
html = html.replace(
    "const current = document.documentElement.getAttribute('data-theme') || 'light';",
    "const current = document.documentElement.getAttribute('data-theme') || 'dark';"
)

# 5. Assemble final file
final_code = pre_html + html_marker + '\n' + html + '\n"""\n\n# ----------------------------------------------------------------------\n# Main\n# ----------------------------------------------------------------------\n' + post_html

with open('client/laptop_server_d_m_light.py', 'w', encoding='utf-8') as f:
    f.write(final_code)

print("SUCCESS: Unified GCS built with all original features and Image 1 formatting!")
